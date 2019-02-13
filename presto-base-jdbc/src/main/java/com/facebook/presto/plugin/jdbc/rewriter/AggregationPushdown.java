/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.jdbc.rewriter;

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcTableLayoutHandle;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorOptimizationRule;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.relation.Aggregate;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ColumnReferenceExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.Project;
import com.facebook.presto.spi.relation.Relation;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.TableScan;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.relational.optimizer.ExpressionOptimizer;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.facebook.presto.plugin.jdbc.rewriter.ExpressionoSqlTranslator.RewriteColumn.rewriteColumns;
import static com.facebook.presto.plugin.jdbc.rewriter.ExpressionoSqlTranslator.RewriteInput.rewriteInput;
import static com.facebook.presto.plugin.jdbc.rewriter.ExpressionoSqlTranslator.fromPrestoType;
import static com.facebook.presto.plugin.jdbc.rewriter.FunctionPattern.function;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AggregationPushdown
        implements ConnectorOptimizationRule
{
    // TODO hack here since we cannot provide FunctionRegistry in SPI
    private final FunctionRegistry functionRegistry = MetadataManager.createTestMetadataManager().getFunctionRegistry();
    private final String connectorId;
    private final TypeManager typeManager;

    public AggregationPushdown(String connectorId, TypeManager typeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public boolean enabled(ConnectorSession session)
    {
        return true;
    }

    @Override
    public boolean match(ConnectorSession session, Relation relation)
    {
        return relation instanceof Aggregate && (
                ((Aggregate) relation).getSource() instanceof TableScan ||
                        (((Aggregate) relation).getSource() instanceof Project &&
                                ((Project) ((Aggregate) relation).getSource()).getSource() instanceof TableScan));
    }

    @Override
    public Optional<Relation> optimize(ConnectorSession session, Relation relation)
    {
        Aggregate aggregate = (Aggregate) relation;
        Optional<Project> project = aggregate.getSource() instanceof Project ? Optional.of((Project) aggregate.getSource()) : Optional.empty();
        TableScan tableScan = (TableScan) (project.isPresent() ? project.get().getSource() : aggregate.getSource());

        // Already has aggregation pushed down
        if (tableScan.getConnectorTableLayoutHandle().isPresent() && ((JdbcTableLayoutHandle) tableScan.getConnectorTableLayoutHandle().get()).getGroupingKeys().size() != 0) {
            return Optional.empty();
        }

        boolean allColumnOutput = tableScan.getOutput()
                .stream()
                .filter(ColumnReferenceExpression.class::isInstance)
                .count() == tableScan.getOutput().size();
        checkArgument(allColumnOutput, "tableScan must has all output as column reference");

        // group key operations must be fully pushed down
        if (aggregate.getGroups().stream()
                .filter(InputReferenceExpression.class::isInstance)
                .count() != aggregate.getGroups().size()) {
            return Optional.empty();
        }

        ExpressionOptimizer optimizer = new ExpressionOptimizer(functionRegistry, typeManager, session);
        RowExpressionRewriter<Result> rewriter = createAggregationRewriter(connectorId,
                new CalculatedColumnIdAllocator(
                        tableScan.getOutput()
                                .stream()
                                .map(ColumnReferenceExpression.class::cast)
                                .map(ColumnReferenceExpression::getColumnHandle)
                                .map(JdbcColumnHandle.class::cast)
                                .map(JdbcColumnHandle::getColumnName)
                                .collect(toImmutableSet())));

        List<RowExpression> aggregations = project.isPresent() ?
                aggregate.getAggregations().stream()
                        .map(rowExpression -> rewriteInput(rowExpression, project.get().getOutput()))
                        .collect(toImmutableList())
                : aggregate.getAggregations();
        aggregations = aggregations.stream()
                .map(rowExpression -> rewriteInput(rowExpression, tableScan.getOutput()))
                .collect(toImmutableList());

        List<Result> results = aggregations.stream()
                .map(call -> rewriter.rewrite(call))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());

        if (results.size() != aggregations.size()) {
            return Optional.empty();
        }

        List<RowExpression> groups = project.isPresent() ?
                aggregate.getGroups().stream()
                        .map(rowExpression -> rewriteInput(rowExpression, project.get().getOutput()))
                        .collect(toImmutableList())
                : aggregate.getGroups();
        groups = groups.stream()
                .map(rowExpression -> rewriteInput(rowExpression, tableScan.getOutput()))
                .collect(toImmutableList());

        // all output columns
        List<ColumnReferenceExpression> columns = Stream.concat(
                groups.stream()
                        .filter(ColumnReferenceExpression.class::isInstance)
                        .map(ColumnReferenceExpression.class::cast),
                results.stream()
                        .map(Result::getColumn))
                .collect(toImmutableList());

        // convert back to input channel
        List<RowExpression> newAggregate = results.stream()
                .map(Result::getAggregate)
                .map(rowExpression -> rewriteColumns(rowExpression, columns))
                .collect(toImmutableList());
        List<RowExpression> newGroups = groups.stream()
                .map(rowExpression -> rewriteColumns(rowExpression, columns))
                .collect(toImmutableList());

        // TODO some function like approx_distinct has literal input, project need to be added to provide constant column
        List<ColumnHandle> groupingKeys = groups
                .stream()
                .map(ColumnReferenceExpression.class::cast)
                .map(ColumnReferenceExpression::getColumnHandle)
                .collect(toImmutableList());

        TableScan newTableScan = new TableScan(
                tableScan.getTableHandle(),
                columns.stream().map(RowExpression.class::cast).collect(toImmutableList()),
                tableScan.getConnectorTableLayoutHandle()
                        .map(JdbcTableLayoutHandle.class::cast)
                        .map(layout -> layout.withGroupingKeys(groupingKeys)));

        if (newTableScan.equals(tableScan)) {
            return Optional.empty();
        }

        return Optional.of(new Aggregate(newAggregate, newGroups, aggregate.getGroupId(), aggregate.getGroupSets(), newTableScan));
    }

    private RowExpressionRewriter<Result> createAggregationRewriter(String connectorId, CalculatedColumnIdAllocator idAllocator)
    {
        return new RowExpressionRewriter<Result>(new FunctionRule.ListBuilder<Result>()
                .add(function("avg").onlyKind(FunctionKind.AGGREGATE), (rewriter, call) -> {
                    checkArgument(call.getArguments().size() == 1, "avg function can has only 1 argument");
                    if (!(call.getArguments().get(0) instanceof ColumnReferenceExpression)) {
                        return null;
                    }
                    InternalAggregationFunction function = functionRegistry.getAggregateFunctionImplementation(call.getSignature());
                    Type intermediateType = function.getIntermediateType();
                    JdbcColumnHandle oldColumn = (JdbcColumnHandle) ((ColumnReferenceExpression) call.getArguments().get(0)).getColumnHandle();
                    String sqlCommand;
                    if (oldColumn.getSqlCommand().isEmpty()) {
                        sqlCommand = oldColumn.getColumnName();
                    }
                    else {
                        checkArgument(oldColumn.getSqlCommand().size() == 1, "Must has only one column for avg");
                        sqlCommand = oldColumn.getSqlCommand().get(0);
                    }

                    List<JdbcTypeHandle> jdbcTypeHandles;
                    if (intermediateType instanceof RowType) {
                        jdbcTypeHandles = ((RowType) intermediateType).getFields()
                                .stream()
                                .map(RowType.Field::getType)
                                .map(ExpressionoSqlTranslator::fromPrestoType)
                                .collect(toImmutableList());
                    }
                    else {
                        jdbcTypeHandles = ImmutableList.of(fromPrestoType(intermediateType));
                    }
                    ColumnReferenceExpression column = new ColumnReferenceExpression(
                            new JdbcColumnHandle(connectorId, idAllocator.getNextId(), oldColumn.getJdbcTypeHandle(), intermediateType, ImmutableList.of(format("sum(%s)", sqlCommand), "count(*)")),
                            intermediateType);
                    RowExpression rewrittenFunction = new CallExpression(call.getSignature(), call.getType(), ImmutableList.of(column));

                    return new Result(rewrittenFunction, column);
                })
                .add(function("count").onlyKind(FunctionKind.AGGREGATE), (rewriter, call) -> {
                    checkArgument(call.getArguments().size() == 1, "avg function can has only 1 argument");
                    if (!(call.getArguments().get(0) instanceof ColumnReferenceExpression)) {
                        return null;
                    }
                    InternalAggregationFunction function = functionRegistry.getAggregateFunctionImplementation(call.getSignature());
                    Type intermediateType = function.getIntermediateType();
                    JdbcColumnHandle oldColumn = (JdbcColumnHandle) ((ColumnReferenceExpression) call.getArguments().get(0)).getColumnHandle();
                    String sqlCommand;
                    if (oldColumn.getSqlCommand().isEmpty()) {
                        sqlCommand = oldColumn.getColumnName();
                    }
                    else {
                        checkArgument(oldColumn.getSqlCommand().size() == 1, "Must has only one column for avg");
                        sqlCommand = oldColumn.getSqlCommand().get(0);
                    }
                    Type type = call.getType();
                    List<JdbcTypeHandle> jdbcTypeHandles;
                    if (type instanceof RowType) {
                        jdbcTypeHandles = ((RowType) type).getFields()
                                .stream()
                                .map(RowType.Field::getType)
                                .map(ExpressionoSqlTranslator::fromPrestoType)
                                .collect(toImmutableList());
                    }
                    else {
                        jdbcTypeHandles = ImmutableList.of(fromPrestoType(type));
                    }
                    ColumnReferenceExpression column = new ColumnReferenceExpression(
                            new JdbcColumnHandle(
                                    connectorId,
                                    idAllocator.getNextId(),
                                    jdbcTypeHandles,
                                    intermediateType,
                                    ImmutableList.of(format("count(%s)", sqlCommand))),
                            intermediateType);
                    RowExpression rewrittenFunction = new CallExpression(
                            new Signature(
                                    "sum",
                                    FunctionKind.AGGREGATE,
                                    call.getType().getTypeSignature(),
                                    ImmutableList.of(call.getType().getTypeSignature())),
                            call.getType(),
                            ImmutableList.of(column));

                    return new Result(rewrittenFunction, column);
                })
                .add(function("sum").onlyKind(FunctionKind.AGGREGATE), unaryForwardingFunction("sum", connectorId, idAllocator))
                .add(function("max").onlyKind(FunctionKind.AGGREGATE), unaryForwardingFunction("max", connectorId, idAllocator))
                .add(function("min").onlyKind(FunctionKind.AGGREGATE), unaryForwardingFunction("min", connectorId, idAllocator))
                .build(),
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.of((rewriter, rowExpression) -> Optional.empty()));
    }

    private BiFunction<RowExpressionRewriter<Result>, CallExpression, Result> unaryForwardingFunction(String sqlFunction, String connectorId, CalculatedColumnIdAllocator idAllocator)
    {
        return (rewriter, call) -> {
            checkArgument(call.getArguments().size() == 1, "avg function can has only 1 argument");
            if (!(call.getArguments().get(0) instanceof ColumnReferenceExpression)) {
                return null;
            }
            InternalAggregationFunction function = functionRegistry.getAggregateFunctionImplementation(call.getSignature());
            Type intermediateType = function.getIntermediateType();
            JdbcColumnHandle oldColumn = (JdbcColumnHandle) ((ColumnReferenceExpression) call.getArguments().get(0)).getColumnHandle();
            String sqlCommand;
            if (oldColumn.getSqlCommand().isEmpty()) {
                sqlCommand = oldColumn.getColumnName();
            }
            else {
                checkArgument(oldColumn.getSqlCommand().size() == 1, "Must has only one column for avg");
                sqlCommand = oldColumn.getSqlCommand().get(0);
            }

            ColumnReferenceExpression column = new ColumnReferenceExpression(
                    new JdbcColumnHandle(connectorId, idAllocator.getNextId(), oldColumn.getJdbcTypeHandle(), intermediateType, ImmutableList.of(format("%s(%s)", sqlFunction, sqlCommand))),
                    intermediateType);
            RowExpression rewrittenFunction = new CallExpression(call.getSignature(), call.getType(), ImmutableList.of(column));

            return new Result(rewrittenFunction, column);
        };
    }

    private static class Result
    {
        private RowExpression aggregate;
        private ColumnReferenceExpression column;

        public Result(RowExpression aggregate, ColumnReferenceExpression column)
        {
            this.aggregate = aggregate;
            this.column = column;
        }

        public RowExpression getAggregate()
        {
            return aggregate;
        }

        public ColumnReferenceExpression getColumn()
        {
            return column;
        }
    }
}
