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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.LiteralInterpreter;
import com.google.common.collect.Streams;

import java.util.Map;

public class RowExpressionMatcher
{
    public boolean match(RowExpression target, RowExpression pattern)
    {
        return target.accept(new Visitor(), pattern);
    }

    protected boolean matchType(Type type, Type typePattern)
    {
        return type.equals(typePattern);
    }

    protected boolean matchFunction(FunctionHandle functionHandle, FunctionHandle functionHandlePattern)
    {
        return functionHandle.equals(functionHandlePattern);
    }

    protected boolean matchValue(Type type, Object value, Object pattern)
    {
        ConnectorSession session;
        Object valueLiteral = LiteralInterpreter.evaluate(session, value);
        Object patternLiteral = LiteralInterpreter.evaluate(session, pattern);
        if (valueLiteral == null || patternLiteral == null) {
            return value == pattern;
        }
        return patternLiteral.equals(valueLiteral);
    }

    protected boolean matchLongLiteral(long value, long pattern)
    {
        return value == pattern;
    }

    private class Visitor
            implements RowExpressionVisitor<Boolean, RowExpression>
    {
        private Map<String, String> variableNameMap;

        @Override
        public Boolean visitCall(CallExpression call, RowExpression pattern)
        {
            if (!(pattern instanceof CallExpression)) {
                return false;
            }
            CallExpression callPattern = (CallExpression) pattern;
            return matchFunction(call.getFunctionHandle(), callPattern.getFunctionHandle()) &&
                    matchType(call.getType(), callPattern.getType()) &&
                    call.getArguments().size() == callPattern.getArguments().size() &&
                    Streams.zip(
                            call.getArguments().stream(),
                            callPattern.getArguments().stream(),
                            (argument, patternArgument) -> argument.accept(this, patternArgument))
                            .allMatch(Boolean::booleanValue);
        }

        @Override
        public Boolean visitInputReference(InputReferenceExpression reference, RowExpression pattern)
        {
            if (!(pattern instanceof InputReferenceExpression)) {
                return false;
            }
            return matchType(reference.getType(), pattern.getType()) && reference.getField() == ((InputReferenceExpression) pattern).getField();
        }

        @Override
        public Boolean visitConstant(ConstantExpression literal, RowExpression pattern)
        {
            if (!(pattern instanceof ConstantExpression)) {
                return false;
            }
            return matchType(literal.getType(), pattern.getType()) && matchValue(literal.getType(), literal.getValue(), ((ConstantExpression) pattern).getValue());
        }

        @Override
        public Boolean visitLambda(LambdaDefinitionExpression lambda, RowExpression pattern)
        {
            return null;
        }

        @Override
        public Boolean visitVariableReference(VariableReferenceExpression reference, RowExpression pattern)
        {
            return null;
        }

        @Override
        public Boolean visitSpecialForm(SpecialFormExpression specialForm, RowExpression pattern)
        {
            if (!(pattern instanceof SpecialFormExpression)) {
                return false;
            }
            SpecialFormExpression specialFormPattern = (SpecialFormExpression) pattern;
            return specialForm.getForm() == specialFormPattern.getForm() &&
                    matchType(specialForm.getType(), specialFormPattern.getType()) &&
                    specialForm.getArguments().size() == specialFormPattern.getArguments().size() &&
                    Streams.zip(
                            specialForm.getArguments().stream(),
                            specialFormPattern.getArguments().stream(),
                            (argument, patternArgument) -> argument.accept(this, patternArgument))
                            .allMatch(Boolean::booleanValue);
        }
    }
}
