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
package com.facebook.presto.optimizer;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;

public abstract class AbstractExpressionTranslator<T>
        implements ExpressionTranslator<TranslatedExpression<T>>
{
    @Override
    public TranslatedExpression<T> translate(RowExpression expression, Map<VariableReferenceExpression, ColumnHandle> columns)
    {
        return expression.accept(new Visitor(), new Context(columns));
    }

    protected Optional<T> translateFunction(CallExpression expression)
    {
        return Optional.empty();
    }

    protected abstract Optional<FunctionTranslator<T>> getFunctionTranslator(FunctionHandle functionHandle);

    protected abstract TranslatedExpression<T> translateColumn(VariableReferenceExpression variableName, ColumnHandle column);

    protected abstract TranslatedExpression<T> translateLiteral(ConstantExpression literal);

    protected TranslatedExpression<T> translateIf(RowExpression originalExpression, List<TranslatedExpression<T>> arguments)
    {
        return TranslatedExpression.untranslated(originalExpression);
    }

    protected TranslatedExpression<T> translateAnd(List<TranslatedExpression<T>> arguments)
    {
        throw new UnsupportedOperationException();
    }

    protected TranslatedExpression<T> translateOr(List<TranslatedExpression<T>> arguments)
    {
        throw new UnsupportedOperationException();
    }

    protected TranslatedExpression<T> translateIn(TranslatedExpression<T> variableName, List<TranslatedExpression<T>> values)
    {
        throw new UnsupportedOperationException();
    }

    private class Context
    {
        private final Map<VariableReferenceExpression, ColumnHandle> columnHandleMap;

        public Context(Map<VariableReferenceExpression, ColumnHandle> columnHandleMap)
        {
            this.columnHandleMap = columnHandleMap;
        }

        public ColumnHandle getColumn(VariableReferenceExpression variable)
        {
            return columnHandleMap.get(variable);
        }
    }

    private class Visitor
            implements RowExpressionVisitor<TranslatedExpression<T>, Context>
    {
        @Override
        public TranslatedExpression<T> visitCall(CallExpression call, Context context)
        {
            Optional<T> defaultTranslatedFunction = translateFunction(call);
            if (defaultTranslatedFunction.isPresent()) {
                return new TranslatedExpression<>(call, defaultTranslatedFunction);
            }
            List<TranslatedExpression<T>> translatedArguments = call.getArguments().stream()
                    .map(expression -> expression.accept(this, context))
                    .collect(toImmutableList());
            Optional<FunctionTranslator<T>> functionTranslator = getFunctionTranslator(call.getFunctionHandle());
            if (functionTranslator.isPresent()) {
                return new TranslatedExpression<>(call, functionTranslator.get().translate(translatedArguments));
            }
            return TranslatedExpression.untranslated(call);
        }

        @Override
        public TranslatedExpression<T> visitInputReference(InputReferenceExpression reference, Context context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TranslatedExpression<T> visitConstant(ConstantExpression literal, Context context)
        {
            return translateLiteral(literal);
        }

        @Override
        public TranslatedExpression<T> visitLambda(LambdaDefinitionExpression lambda, Context context)
        {
            return TranslatedExpression.untranslated(lambda);
        }

        @Override
        public TranslatedExpression<T> visitVariableReference(VariableReferenceExpression reference, Context context)
        {
            return translateColumn(reference, context.getColumn(reference));
        }

        @Override
        public TranslatedExpression<T> visitSpecialForm(SpecialFormExpression specialForm, Context context)
        {
            List<TranslatedExpression<T>> translatedArguments = specialForm.getArguments().stream()
                    .map(expression -> expression.accept(this, context))
                    .collect(toImmutableList());
            switch (specialForm.getForm()) {
                case IF:
                    return translateIf(specialForm, translatedArguments);
                case NULL_IF:
                case SWITCH:
                case WHEN:
                case IS_NULL:
                case COALESCE:
                case IN:
                    return translateIn(translatedArguments.get(0), translatedArguments.subList(1, translatedArguments.size()));
                case AND:
                    return translateAnd(translatedArguments);
                case OR:
                    return translateOr(translatedArguments);
                case DEREFERENCE:
                case ROW_CONSTRUCTOR:
                case BIND:
                    return TranslatedExpression.untranslated(specialForm);
            }
            throw new UnsupportedOperationException();
        }
    }
}
