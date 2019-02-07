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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.plugin.jdbc.rewriter.ColumnRule;
import com.facebook.presto.plugin.jdbc.rewriter.ConstantRule;
import com.facebook.presto.plugin.jdbc.rewriter.FunctionRule;
import com.facebook.presto.plugin.jdbc.rewriter.RowExpressionRewriter;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.relation.Aggregate;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ColumnReferenceExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.Filter;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.Project;
import com.facebook.presto.spi.relation.Relation;
import com.facebook.presto.spi.relation.RelationVisitor;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.TableScan;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.sql.Types;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.plugin.jdbc.PushdownCalculationToMySql.RewriteInput.rewriteInput;
import static com.facebook.presto.plugin.jdbc.rewriter.FunctionPattern.function;
import static com.facebook.presto.plugin.jdbc.rewriter.FunctionPattern.operator;
import static com.facebook.presto.spi.function.OperatorType.ADD;
import static com.facebook.presto.spi.function.OperatorType.CAST;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.function.OperatorType.SUBTRACT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

public class PushdownCalculationToMySql
{

    private final RowExpressionRewriter<List<String>> toSqlRewriter;
    private final RowExpressionRewriter<RowExpression> rowExpressionRewriter;

    public PushdownCalculationToMySql(String connectorId)
    {
        toSqlRewriter = new RowExpressionRewriter<List<String>>(
                new FunctionRule.ListBuilder<List<String>>()
                        .add(function("sum"), (currentRewriter, arguments) -> {
                            checkArgument(arguments.size() == 1);
                            Optional<List<String>> child = currentRewriter.rewrite(arguments.get(0));
                            if (!child.isPresent()) {
                                return Optional.empty();
                            }
                            return Optional.of(
                                    ImmutableList.of(format("sum(%s)", child.get().get(0))));
                        })
                        .add(function("avg"), (currentRewriter, arguments) -> {
                            checkArgument(arguments.size() == 1);
                            Optional<List<String>> child = currentRewriter.rewrite(arguments.get(0));
                            if (!child.isPresent()) {
                                return Optional.empty();
                            }
                            return Optional.of(
                                    ImmutableList.of(
                                            format("sum(%s)", child.get().get(0)),
                                            "count(*)"));
                        })
                        .add(function("and"), (currentRewriter, arguments) -> {
                            checkArgument(arguments.size() == 2);
                            Optional<List<String>> left = currentRewriter.rewrite(arguments.get(0));
                            Optional<List<String>> right = currentRewriter.rewrite(arguments.get(1));
                            if (left.isPresent() && right.isPresent()) {
                                return Optional.of(ImmutableList.of(format("(%s) AND (%s)", left.get().get(0), right.get().get(0))));
                            }
                            return Optional.empty();
                        })
                        .add(function("IN"), (currentRewriter, arguments) -> {
                            List<String> results = arguments.stream()
                                    .map(list -> currentRewriter.rewrite(list))
                                    .filter(Optional::isPresent)
                                    .filter(list -> list.get().size() == 1)
                                    .map(list -> list.get().get(0))
                                    .collect(toImmutableList());
                            if (results.size() == arguments.size()) {
                                return Optional.of(ImmutableList.of(format(" (%s) IN (%s)", results.get(0), Joiner.on(",").join(results.subList(1, results.size())))));
                            }
                            return Optional.empty();
                        })
                        .add(operator(ADD), (currentRewriter, arguments) -> {
                            checkArgument(arguments.size() == 2);
                            Optional<List<String>> left = currentRewriter.rewrite(arguments.get(0));
                            Optional<List<String>> right = currentRewriter.rewrite(arguments.get(1));
                            if (left.isPresent() && right.isPresent()) {
                                return Optional.of(ImmutableList.of(format("(%s) + (%s)", left.get().get(0), right.get().get(0))));
                            }
                            return Optional.empty();
                        })
                        .add(operator(SUBTRACT), (currentRewriter, arguments) -> {
                            Optional<List<String>> left = currentRewriter.rewrite(arguments.get(0));
                            Optional<List<String>> right = currentRewriter.rewrite(arguments.get(1));
                            if (left.isPresent() && right.isPresent()) {
                                return Optional.of(ImmutableList.of(format("(%s) - (%s)", left.get().get(0), right.get().get(0))));
                            }
                            return Optional.empty();
                        })
                        .add(operator(GREATER_THAN), (currentRewriter, arguments) -> {
                            Optional<List<String>> left = currentRewriter.rewrite(arguments.get(0));
                            Optional<List<String>> right = currentRewriter.rewrite(arguments.get(1));
                            if (left.isPresent() && right.isPresent()) {
                                return Optional.of(ImmutableList.of(format("(%s) > (%s)", left.get().get(0), right.get().get(0))));
                            }
                            return Optional.empty();
                        })
                        .add(operator(CAST), (currentRewriter, arguments) -> {
                            Optional<List<String>> value = currentRewriter.rewrite(arguments.get(0));
                            if (value.isPresent()) {
                                return Optional.of(ImmutableList.of(format("CAST(%s AS %s)", value.get().get(0), "VARCHAR")));
                            }
                            return Optional.empty();
                        })
                        .build(),
                new ConstantRule.ListBuilder<List<String>>()
                        .add(BIGINT, object -> Optional.of(ImmutableList.of(format("%s", object))))
                        .add(INTEGER, object -> Optional.of(ImmutableList.of(format("%s", object))))
                        .add(VARCHAR, object -> Optional.of(ImmutableList.of(format("\'%s\'", ((Slice) object).toStringUtf8()))))
                        .build(),
                new ColumnRule.ListBuilder<List<String>>()
                        .add(JdbcColumnHandle.class, columnHandle -> Optional.of(ImmutableList.of(format("`%s`", ((JdbcColumnHandle) columnHandle).getColumnName()))))
                        .build());

        rowExpressionRewriter = new RowExpressionRewriter<RowExpression>((rowExpression, mywriter) -> {
            if (rowExpression instanceof CallExpression) {
                Optional<List<String>> sql = toSqlRewriter.rewrite(rowExpression);
                if (sql.isPresent()) {
                    return Optional.of(
                            new ColumnReferenceExpression(
                                    new JdbcColumnHandle(connectorId, "composite_column", fromPrestoType(rowExpression.getType()), rowExpression.getType(), sql.get()), rowExpression.getType()));
                    // if function is aggregation, it has to be root
                }
                CallExpression callExpression = (CallExpression) rowExpression;
                boolean anyChildRewritten = false;
                ImmutableList.Builder<RowExpression> newChildren = ImmutableList.builder();
                for (int i = 0; i < callExpression.getArguments().size(); i++) {
                    Optional<RowExpression> r = mywriter.rewrite(callExpression.getArguments().get(i));
                    if (r.isPresent()) {
                        anyChildRewritten = true;
                    }
                    newChildren.add(r.orElse(callExpression.getArguments().get(i)));
                }
                if (anyChildRewritten) {
                    return Optional.of(new CallExpression(callExpression.getSignature(), callExpression.getType(), newChildren.build()));
                }
            }
            return Optional.of(rowExpression);
        });

//        RowExpressionRewriter<RowExpression> predicateRewriter = new RowExpressionRewriter<RowExpression>(
//                new FunctionRule.ListBuilder<List<String>>()
//                        .add(function("and").returns(BooleanType.BOOLEAN),  (currentRewriter, arguments) -> {
//                            Optional<List<String>> leftSql = toSqlRewriter.rewrite(arguments.get(0));
//                            Optional<List<String>> rightSql = toSqlRewriter.rewrite(arguments.get(1));
//                            if (leftSql.isPresent())  {
//
//                            }
//                            return Optional.empty();
//                        })
//                        .build(),
//                ImmutableList.of(),
//                ImmutableList.of(),
//                Optional.empty());
    }

    private JdbcTypeHandle fromPrestoType(Type type)
    {
        if (type.equals(BIGINT)) {
            return new JdbcTypeHandle(Types.BIGINT, 11, 0);
        }
        else if (type.equals(BOOLEAN)) {
            return new JdbcTypeHandle(Types.BOOLEAN, 1, 0);
        }
        else if (type instanceof VarcharType) {
            return new JdbcTypeHandle(Types.VARCHAR, ((VarcharType) type).getLengthSafe(), 0);
        }
        throw new UnsupportedOperationException(format("Unsupported type: %s", type));
    }

    public Optional<Relation> rewrite(Relation relation)
    {
        return relation.accept(new Visitor(), null);
    }

    public class Visitor
            extends RelationVisitor<Optional<Relation>, Void>
    {
        @Override
        protected Optional<Relation> visitProject(Project project, Void context)
        {
            Optional<Relation> child = project.getSource().accept(this, context);
            return Optional.empty();
        }

        @Override
        protected Optional<Relation> visitFilter(Filter filter, Void context)
        {
            Optional<Relation> child = filter.getSource().accept(this, context);
            RowExpression predicate = rewriteInput(filter.getPredicate(), filter.getSource().getOutput());
            Optional<RowExpression> rewritten = rowExpressionRewriter.rewrite(predicate);
            if (child.isPresent() && child.get() instanceof TableScan) {
                TableScan tableScan = (TableScan) child.get();
                tableScan.withTableLayout(
                        new JdbcTableLayoutHandle(
                                (JdbcTableHandle) tableScan.getTableHandle(),
                                TupleDomain.all(), Optional.empty()));
            }
            return Optional.empty();
        }

        @Override
        protected Optional<Relation> visitAggregate(Aggregate aggregate, Void context)
        {
            Optional<Relation> child = aggregate.getSource().accept(this, context);
            return Optional.empty();
        }

        @Override
        protected Optional<Relation> visitTableScan(TableScan tableScan, Void context)
        {
            return Optional.of(tableScan);
        }
    }

    public static class RewriteInput
            implements RowExpressionVisitor<RowExpression, Void>
    {
        private final List<RowExpression> inputChannel;

        public RewriteInput(List<RowExpression> inputChannel)
        {
            this.inputChannel = inputChannel;
        }

        public static RowExpression rewriteInput(RowExpression rowExpression, List<RowExpression> inputChannel)
        {
            return rowExpression.accept(new RewriteInput(inputChannel), null);
        }

        @Override
        public RowExpression visitCall(CallExpression call, Void context)
        {
            return new CallExpression(call.getSignature(),
                    call.getType(),
                    call.getArguments()
                            .stream()
                            .map(row -> row.accept(this, context))
                            .collect(toImmutableList()));
        }

        @Override
        public RowExpression visitInputReference(InputReferenceExpression reference, Void context)
        {
            int field = reference.getField();
            if (field >= 0 && field < inputChannel.size()) {
                return inputChannel.get(field);
            }
            return reference;
        }

        @Override
        public RowExpression visitConstant(ConstantExpression literal, Void context)
        {
            return literal;
        }

        @Override
        public RowExpression visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            return null;
        }

        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return null;
        }
    }

    public class CountInput
            implements RowExpressionVisitor<Integer, Void>
    {
        @Override
        public Integer visitCall(CallExpression call, Void context)
        {
            return call.getArguments().stream().mapToInt(row -> row.accept(this, context)).sum();
        }

        @Override
        public Integer visitInputReference(InputReferenceExpression reference, Void context)
        {
            return 1;
        }

        @Override
        public Integer visitConstant(ConstantExpression literal, Void context)
        {
            return 0;
        }

        @Override
        public Integer visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            return lambda.getBody().accept(this, context);
        }

        @Override
        public Integer visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return 0;
        }
    }
}
