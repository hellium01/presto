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

import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorOptimizationRule;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ColumnReferenceExpression;
import com.facebook.presto.spi.relation.Filter;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.Project;
import com.facebook.presto.spi.relation.Relation;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.TableScan;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.plugin.jdbc.rewriter.ExpressionoSqlTranslator.RewriteColumn.rewriteColumns;
import static com.facebook.presto.plugin.jdbc.rewriter.ExpressionoSqlTranslator.RewriteInput.rewriteInput;
import static com.facebook.presto.plugin.jdbc.rewriter.ExpressionoSqlTranslator.fromPrestoType;
import static com.facebook.presto.plugin.jdbc.rewriter.ExpressionoSqlTranslator.translateToSQL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class ProjectPushdown
        implements ConnectorOptimizationRule
{
    private final String connectorId;

    public ProjectPushdown(String connectorId)
    {
        this.connectorId = connectorId;
    }

    @Override
    public boolean enabled(ConnectorSession session)
    {
        return true;
    }

    @Override
    public boolean match(ConnectorSession session, Relation relation)
    {
        return relation instanceof Project && (
                ((Project) relation).getSource() instanceof TableScan ||
                        (((Project) relation).getSource() instanceof Filter && ((Filter) ((Project) relation).getSource()).getSource() instanceof TableScan));
    }

    @Override
    public Optional<Relation> optimize(ConnectorSession session, Relation relation)
    {
        Project project = (Project) relation;
        Optional<Filter> filter = project.getSource() instanceof Filter ? Optional.of((Filter) project.getSource()) : Optional.empty();
        TableScan tableScan = (TableScan) filter.map(Filter::getSource).orElse(project.getSource());
        boolean allColumnOutput = tableScan.getOutput()
                .stream()
                .filter(ColumnReferenceExpression.class::isInstance)
                .count() == tableScan.getOutput().size();
        checkArgument(allColumnOutput, "tableScan must has all output as column reference");

        // rewrite from channel to column to be able to push down to tableScan
        List<RowExpression> rowExpressions = project.getOutput().stream()
                .map(rowExpression -> rewriteInput(rowExpression, tableScan.getOutput()))
                .collect(Collectors.toList());
        CalculatedColumnIdAllocator idAllocator = new CalculatedColumnIdAllocator();

        // rewrite, leftover will be used in
        List<Result> results = rowExpressions.stream()
                .map(rowExpression -> getProjectRewriter(connectorId, idAllocator).rewrite(rowExpression))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());

        List<ColumnReferenceExpression> columns = results.stream()
                .map(Result::getColumns)
                .flatMap(List::stream)
                .collect(toImmutableSet())
                .asList();

        TableScan newTableScan = new TableScan(
                tableScan.getTableHandle(),
                columns.stream()
                        .map(RowExpression.class::cast)
                        .collect(toImmutableList()),
                tableScan.getConnectorTableLayoutHandle());

        // covert back leftover from column reference to channel
        List<RowExpression> outputExpressions = results.stream()
                .map(result -> rewriteColumns(result.getLeftOver(), columns))
                .collect(Collectors.toList());

        if (outputExpressions.stream().filter(InputReferenceExpression.class::isInstance).count() < outputExpressions.size()) {
            if (newTableScan.equals(tableScan) && outputExpressions.equals(project.getOutput())) {
                return Optional.empty();
            }
            return Optional.of(new Project(outputExpressions, newTableScan));
        }
        return Optional.of(newTableScan);
    }

    private RowExpressionRewriter<Result> getProjectRewriter(String connectorId, CalculatedColumnIdAllocator idAllocator)
    {
        return new RowExpressionRewriter<Result>(
                (rewriter, rowExpression) -> {
                    if (rowExpression instanceof ColumnReferenceExpression) {
                        return Optional.of(new Result(rowExpression, ImmutableList.of(((ColumnReferenceExpression) rowExpression))));
                    }
                    else if (!(rowExpression instanceof CallExpression)) {
                        return Optional.of(new Result(rowExpression, ImmutableList.of()));
                    }

                    CallExpression callExpression = (CallExpression) rowExpression;
                    Optional<String> rewritten = translateToSQL(rowExpression);
                    if (rewritten.isPresent()) {
                        Type type = rowExpression.getType();
                        ColumnHandle handle = new JdbcColumnHandle(
                                connectorId,
                                idAllocator.getNextId(),
                                fromPrestoType(type),
                                type,
                                Splitter.on(";").splitToList(rewritten.get()));
                        ColumnReferenceExpression columnReference = new ColumnReferenceExpression(handle, type);
                        return Optional.of(new Result(columnReference, ImmutableList.of(columnReference)));
                    }
                    List<Result> result = callExpression.getArguments()
                            .stream()
                            .map(argument -> rewriter.rewrite(argument))
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .collect(toImmutableList());
                    checkArgument(result.size() == callExpression.getArguments().size(), "Rewritten result must be same as call arguments");
                    List<ColumnReferenceExpression> columnHandles = result.stream()
                            .map(Result::getColumns)
                            .flatMap(List::stream)
                            .collect(toImmutableList());
                    return Optional.of(
                            new Result(
                                    new CallExpression(
                                            callExpression.getSignature(),
                                            callExpression.getType(),
                                            result
                                                    .stream()
                                                    .map(Result::getLeftOver)
                                                    .collect(toImmutableList())
                                    ),
                                    columnHandles));
                });
    }

    private static class Result
    {
        private final RowExpression leftOver;
        private final List<ColumnReferenceExpression> columns;

        public Result(RowExpression leftOver, List<ColumnReferenceExpression> columns)
        {
            this.leftOver = requireNonNull(leftOver, "leftOver is nul");
            this.columns = requireNonNull(columns, "columns is null");
        }

        public RowExpression getLeftOver()
        {
            return leftOver;
        }

        public List<ColumnReferenceExpression> getColumns()
        {
            return columns;
        }
    }
}
