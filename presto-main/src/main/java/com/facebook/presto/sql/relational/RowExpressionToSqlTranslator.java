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
package com.facebook.presto.sql.relational;

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ColumnReferenceExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.LiteralEncoder;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.metadata.FunctionRegistry.isOperator;
import static com.facebook.presto.metadata.FunctionRegistry.unmangleOperator;
import static com.google.common.base.Preconditions.checkArgument;

public class RowExpressionToSqlTranslator
{

    public static Optional<Expression> translate(RowExpression rowExpression, List<Symbol> inputs, Map<ColumnHandle, String> columns, LiteralEncoder literalEncoder,
            FunctionRegistry functionRegistry)
    {
        return rowExpression.accept(new Visitor(inputs, columns, literalEncoder, functionRegistry), null);
    }

    public static class Visitor
            implements RowExpressionVisitor<Optional<Expression>, Void>
    {
        private final List<Symbol> inputs;
        private final Map<ColumnHandle, String> columns;
        private final LiteralEncoder literalEncoder;
        private final FunctionRegistry functionRegistry;

        public Visitor(List<Symbol> inputs, Map<ColumnHandle, String> columns, LiteralEncoder literalEncoder, FunctionRegistry functionRegistry)
        {
            this.inputs = inputs;
            this.columns = columns;
            this.literalEncoder = literalEncoder;
            this.functionRegistry = functionRegistry;
        }

        @Override
        public Optional<Expression> visitCall(CallExpression call, Void context)
        {
            // TODO convert back the function is very hacky at the moment
            List<Expression> arguments = call.getArguments()
                    .stream()
                    .map(rowExpression -> rowExpression.accept(this, context))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());
            if (arguments.size() != call.getArguments().size()) {
                return Optional.empty();
            }

            if (call.getSignature().getName().equalsIgnoreCase("AND")) {
                checkArgument(arguments.size() == 2, "LogicalBinaryExpression AND must have 2 arguments");
                return Optional.of(new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND, arguments.get(0), arguments.get(1)));
            }
            else if (call.getSignature().getName().equalsIgnoreCase("OR")) {
                checkArgument(arguments.size() == 2, "LogicalBinaryExpression OR must have 2 arguments");
                return Optional.of(new LogicalBinaryExpression(LogicalBinaryExpression.Operator.OR, arguments.get(0), arguments.get(1)));
            }
            else if (call.getSignature().getName().equalsIgnoreCase("IN")) {
                return Optional.of(new InListExpression(arguments));
            }
            else if (call.getSignature().getName().equalsIgnoreCase("IF")) {
                return Optional.of(new IfExpression(arguments.get(0), arguments.get(1), arguments.get(2)));
            }

            if (isOperator(call.getSignature().getName())) {
                switch (unmangleOperator(call.getSignature().getName())) {
                    case ADD:
                        return Optional.of(new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.ADD, arguments.get(0), arguments.get(1)));
                    case SUBTRACT:
                        return Optional.of(new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.SUBTRACT, arguments.get(0), arguments.get(1)));
                    case DIVIDE:
                        return Optional.of(new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.DIVIDE, arguments.get(0), arguments.get(1)));
                    case MULTIPLY:
                        return Optional.of(new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.MULTIPLY, arguments.get(0), arguments.get(1)));
                    case MODULUS:
                        return Optional.of(new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.MODULUS, arguments.get(0), arguments.get(1)));
                    case NEGATION:
                        return Optional.of(new ArithmeticUnaryExpression(ArithmeticUnaryExpression.Sign.MINUS, arguments.get(0)));
                    case EQUAL:
                        return Optional.of(new ComparisonExpression(ComparisonExpression.Operator.EQUAL, arguments.get(0), arguments.get(1)));
                    case NOT_EQUAL:
                        return Optional.of(new ComparisonExpression(ComparisonExpression.Operator.NOT_EQUAL, arguments.get(0), arguments.get(1)));
                    case LESS_THAN:
                        return Optional.of(new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN, arguments.get(0), arguments.get(1)));
                    case LESS_THAN_OR_EQUAL:
                        return Optional.of(new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN_OR_EQUAL, arguments.get(0), arguments.get(1)));
                    case GREATER_THAN:
                        return Optional.of(new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN, arguments.get(0), arguments.get(1)));
                    case GREATER_THAN_OR_EQUAL:
                        return Optional.of(new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL, arguments.get(0), arguments.get(1)));
                    case BETWEEN:
                        return Optional.of(new LogicalBinaryExpression(
                                LogicalBinaryExpression.Operator.AND,
                                new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL, arguments.get(0), arguments.get(1)),
                                new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN_OR_EQUAL, arguments.get(0), arguments.get(2))));
                    case CAST:
                        return Optional.of(new Cast(arguments.get(0), call.getType().getTypeSignature().getBase()));
                    case SUBSCRIPT:
                        return Optional.of(new SubscriptExpression(arguments.get(0), arguments.get(1)));
                    default:
                        return Optional.empty();
                }
            }

            List<QualifiedName> functions = functionRegistry.getFunctions(call.getSignature());
            if (functions.size() == 0) {
                return Optional.empty();
            }
            return Optional.of(new FunctionCall(functions.get(0), arguments));
        }

        @Override
        public Optional<Expression> visitInputReference(InputReferenceExpression reference, Void context)
        {
            checkArgument(reference.getField() < inputs.size(), "field not found.");
            return Optional.of(inputs.get(reference.getField()).toSymbolReference());
        }

        @Override
        public Optional<Expression> visitConstant(ConstantExpression literal, Void context)
        {
            return Optional.of(literalEncoder.toExpression(literal.getValue(), literal.getType()));
        }

        @Override
        public Optional<Expression> visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            // TODO support lambada
            return Optional.empty();
        }

        @Override
        public Optional<Expression> visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return Optional.of(new SymbolReference(reference.getName()));
        }

        @Override
        public Optional<Expression> visitColumnReference(ColumnReferenceExpression columnReferenceExpression, Void context)
        {
            checkArgument(columns.containsKey(columnReferenceExpression.getColumnHandle()), "columnHandle not found.");
            return Optional.of(new SymbolReference(columns.get(columnReferenceExpression.getColumnHandle())));
        }
    }
}
