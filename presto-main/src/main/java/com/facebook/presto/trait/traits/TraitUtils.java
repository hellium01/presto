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
package com.facebook.presto.trait.traits;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.CastType;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.RowExpressionInterpreter;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.relational.RowExpressionUtils.inlineExpressions;
import static com.google.common.collect.ImmutableList.builder;
import static java.util.stream.Collectors.toMap;

public class TraitUtils
{
    private TraitUtils()
    {
    }

    public static RowFilterTrait filterTrait(RowExpression predicate)
    {
        return new RowFilterTrait(predicate);
    }

    public static CollationTrait orderTrait(List<VariableReferenceExpression> variables, List<Order> order)
    {
        return new CollationTrait(variables, order);
    }

    public static ConstantTrait constantTrait(String name, Type type, Object value)
    {
        return new ConstantTrait(new VariableReferenceExpression(name, type), value);
    }

    public static CollationTrait unordered()
    {
        return new CollationTrait(ImmutableList.of(), ImmutableList.of());
    }

    public static List<ConstantTrait> transform(Metadata metadata, Session session, Map<VariableReferenceExpression, RowExpression> assignments, List<ConstantTrait> constantTraits)
    {
        ImmutableList.Builder<ConstantTrait> builder = builder();
        Map<RowExpression, RowExpression> inputConstants = constantTraits.stream()
                .collect(toMap(ConstantTrait::getVariable, ConstantTrait::getValue));
        for (Map.Entry<VariableReferenceExpression, RowExpression> assignment : assignments.entrySet()) {
            RowExpression expression = assignment.getValue();
            VariableReferenceExpression variable = assignment.getKey();
            if (expression.getType() != variable.getType()) {
                expression = cast(metadata.getFunctionManager(), expression, variable.getType());
            }
            expression = inlineExpressions(expression, inputConstants);
            // should use optimize so that nondeterministic expression will not be evaluated into constant
            // there still some constant expression (regex for example) cannot be evaluated into constant
            Object value = new RowExpressionInterpreter(expression, metadata, session, true).optimize();
            if (value instanceof RowExpression) {
                continue;
            }
            builder.add(constantTrait(variable.getName(), variable.getType(), value));
        }
        return builder.build();
    }

    private static RowExpression cast(FunctionManager functionManager, RowExpression expression, Type toType)
    {
        FunctionHandle cast = functionManager.lookupCast(CastType.CAST, expression.getType().getTypeSignature(), toType.getTypeSignature());
        return new CallExpression(cast, toType, ImmutableList.of(expression));
    }
}
