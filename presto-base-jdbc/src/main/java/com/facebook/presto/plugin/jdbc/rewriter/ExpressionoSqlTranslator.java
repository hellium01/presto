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
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ColumnReferenceExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Joiner;
import io.airlift.slice.Slice;

import java.sql.Types;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.facebook.presto.plugin.jdbc.rewriter.FunctionPattern.function;
import static com.facebook.presto.plugin.jdbc.rewriter.FunctionPattern.operator;
import static com.facebook.presto.spi.function.OperatorType.ADD;
import static com.facebook.presto.spi.function.OperatorType.CAST;
import static com.facebook.presto.spi.function.OperatorType.DIVIDE;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.MULTIPLY;
import static com.facebook.presto.spi.function.OperatorType.SUBTRACT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

public class ExpressionoSqlTranslator
{
    private static RowExpressionRewriter<String> TO_SQL_REWRITER = getSqlRewriter();

    public static Optional<String> translateToSQL(RowExpression rowExpression)
    {
        return TO_SQL_REWRITER.rewrite(rowExpression);
    }

    private static RowExpressionRewriter<String> getSqlRewriter()
    {
        return new RowExpressionRewriter<String>(
                new FunctionRule.ListBuilder<String>()
                        .add(function("sum"), unaryFunction("sum"))
                        .add(function("count"), unaryFunction("count"))
                        .add(function("min"), unaryFunction("min"))
                        .add(function("max"), unaryFunction("max"))
                        .add(function("avg"), (rewriter, callExpression) -> {
                            List<RowExpression> arguments = callExpression.getArguments();
                            checkArgument(arguments.size() == 1, "must has only 1 argument for function avg");
                            Optional<String> child = rewriter.rewrite(arguments.get(0));
                            if (child.isPresent()) {
                                return format("sum(%s);count(*)", child.get());
                            }
                            return null;
                        })
                        .add(function("IN"), (rewriter, callExpression) -> {
                            List<RowExpression> arguments = callExpression.getArguments();
                            List<String> results = arguments.stream()
                                    .map(list -> rewriter.rewrite(list))
                                    .filter(Optional::isPresent)
                                    .map(Optional::get)
                                    .collect(toImmutableList());
                            if (results.size() == arguments.size()) {
                                return format("(%s) IN (%s)", results.get(0), Joiner.on(",").join(results.subList(1, results.size())));
                            }
                            return null;
                        })
                        .add(function("and"), binaryFunction("AND"))
                        .add(function("or"), binaryFunction("OR"))
                        .add(function("if"), (rewriter, callExpression) -> {
                            List<RowExpression> arguments = callExpression.getArguments();
                            checkArgument(arguments.size() == 3, "must has only 3 argument for IF");
                            Optional<String> condition = rewriter.rewrite(arguments.get(0));
                            Optional<String> trueValue = rewriter.rewrite(arguments.get(1));
                            Optional<String> falseValue = rewriter.rewrite(arguments.get(2));
                            if (condition.isPresent() && trueValue.isPresent() && falseValue.isPresent()) {
                                return format("IF((%s),(%s),(%s))", condition.get(), trueValue.get(), falseValue.get());
                            }
                            return null;
                        })
                        .add(operator(ADD), binaryFunction("+"))
                        .add(operator(SUBTRACT), binaryFunction("-"))
                        .add(operator(MULTIPLY), binaryFunction("*"))
                        .add(operator(DIVIDE), binaryFunction("/"))
                        .add(operator(GREATER_THAN), binaryFunction(">"))
                        .add(operator(GREATER_THAN_OR_EQUAL), binaryFunction(">="))
                        .add(operator(LESS_THAN), binaryFunction("<"))
                        .add(operator(LESS_THAN_OR_EQUAL), binaryFunction("<="))
                        .add(operator(CAST), (rewriter, callExpression) -> {
                            List<RowExpression> arguments = callExpression.getArguments();
                            checkArgument(arguments.size() == 1, "must has only 1 argument for cast");
                            Optional<String> value = rewriter.rewrite(callExpression.getArguments().get(0));
                            if (value.isPresent()) {
                                return format("CAST(%s AS %s)", value.get(), toSqlType(fromPrestoType(callExpression.getType()).getJdbcType()));
                            }
                            return null;
                        })
                        .build(),
                new ConstantRule.ListBuilder<String>()
                        .add(BIGINT, object -> object == null ? Optional.of("NULL") : Optional.of(format("%s", object)))
                        .add(INTEGER, object -> object == null ? Optional.of("NULL") : Optional.of(format("%s", object)))
                        .add(VARCHAR, object -> object == null ? Optional.of("NULL") : Optional.of(format("\'%s\'", ((Slice) object).toStringUtf8())))
                        .build(),
                new ColumnRule.ListBuilder<String>()
                        .add(JdbcColumnHandle.class, columnHandle -> Optional.of(format("`%s`", ((JdbcColumnHandle) columnHandle).getColumnName())))
                        .build());
    }

    private static BiFunction<RowExpressionRewriter<String>, CallExpression, String> unaryFunction(String functionName)
    {
        return (rewriter, callExpression) -> {
            List<RowExpression> arguments = callExpression.getArguments();
            checkArgument(arguments.size() == 1, "must has only 1 argument for function %s", functionName);
            Optional<String> child = rewriter.rewrite(arguments.get(0));
            if (child.isPresent()) {
                return format("%s(%s)", functionName, child.get());
            }
            return null;
        };
    }

    private static BiFunction<RowExpressionRewriter<String>, CallExpression, String> binaryFunction(String functionName)
    {
        return (rewriter, callExpression) -> {
            List<RowExpression> arguments = callExpression.getArguments();
            checkArgument(arguments.size() == 2, "must has only 2 argument for function/operator %s", functionName);
            Optional<String> left = rewriter.rewrite(arguments.get(0));
            Optional<String> right = rewriter.rewrite(arguments.get(1));
            if (left.isPresent() && right.isPresent()) {
                return format("((%s) %s (%s))", left.get(), functionName, right.get());
            }
            return null;
        };
    }

    public static JdbcTypeHandle fromPrestoType(Type type)
    {
        if (type.equals(BigintType.BIGINT)) {
            return new JdbcTypeHandle(Types.BIGINT, 11, 0);
        }
        else if (type.equals(BooleanType.BOOLEAN)) {
            return new JdbcTypeHandle(Types.BOOLEAN, 1, 0);
        }
        else if (type.equals(DoubleType.DOUBLE)) {
            return new JdbcTypeHandle(Types.DOUBLE, 11, 0);
        }
        else if (type instanceof VarcharType) {
            return new JdbcTypeHandle(Types.VARCHAR, ((VarcharType) type).getLengthSafe(), 0);
        }
        throw new UnsupportedOperationException(format("Unsupported type: %s", type));
    }

    public static String toSqlType(int jdbcType)
    {
        switch (jdbcType) {
            case Types.BIGINT:
                return "BIGINT";
            case Types.BOOLEAN:
                return "BOOLEAN";
            case Types.VARCHAR:
                return "VARCHAR";
        }
        throw new UnsupportedOperationException(format("Unsupported jdbc type: %s", jdbcType));
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

    public static class RewriteColumn
            implements RowExpressionVisitor<RowExpression, Void>
    {
        private final List<ColumnReferenceExpression> inputColumns;

        public RewriteColumn(List<ColumnReferenceExpression> inputColumns)
        {
            this.inputColumns = inputColumns;
        }

        public static RowExpression rewriteColumns(RowExpression rowExpression, List<ColumnReferenceExpression> inputColumns)
        {
            return rowExpression.accept(new RewriteColumn(inputColumns), null);
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

        @Override
        public RowExpression visitColumnReference(ColumnReferenceExpression columnReferenceExpression, Void context)
        {
            int i = inputColumns.indexOf(columnReferenceExpression);
            return new InputReferenceExpression(i, columnReferenceExpression.getType());
        }
    }
}
