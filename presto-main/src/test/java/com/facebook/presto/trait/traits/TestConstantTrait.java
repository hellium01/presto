

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

import com.facebook.presto.metadata.CastType;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.StandardFunctionResolution;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.trait.TraitSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.variable;
import static com.facebook.presto.trait.traits.ConstantTraitType.CONSTANT_TRAIT_TYPE;
import static com.facebook.presto.trait.traits.TraitUtils.constantTrait;
import static com.facebook.presto.type.JoniRegexpType.JONI_REGEXP;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;

public class TestConstantTrait
{
    private Metadata metadata;
    private StandardFunctionResolution resolution;

    @BeforeMethod
    public void setUp()
    {
        metadata = createTestMetadataManager();
        resolution = new StandardFunctionResolution(metadata.getFunctionManager());
    }

    @Test
    public void testMultipleConstant()
    {
        TraitSet traits = TraitSet.emptyTraitSet();
        traits.add(constantTrait("c1", BIGINT, 1L));
        traits.add(constantTrait("c2", BIGINT, 1L));
        traits.add(constantTrait("c2", BIGINT, 4L));
        assertEqualsIgnoreOrder(traits.get(CONSTANT_TRAIT_TYPE),
                ImmutableList.of(
                        constantTrait("c1", BIGINT, 1L),
                        constantTrait("c2", BIGINT, 4L)));
    }

    @Test
    public void testTransform()
    {
        TraitSet traits = TraitSet.emptyTraitSet();
        traits.add(constantTrait("c1", BIGINT, 1L));
        traits.add(constantTrait("c2", BIGINT, 1L));
        traits.add(constantTrait("c2", BIGINT, 4L));
        List<ConstantTrait> result = transform(ImmutableMap.of(
                variable("c3", BIGINT),
                add(add(variable("c1", BIGINT), variable("c2", BIGINT)), constant(1L, BIGINT)),
                variable("c4", BIGINT),
                add(variable("c1", BIGINT), rand(10)),
                variable("c5", VARCHAR),
                regexp_extract("ABC", "A")),
                traits.get(CONSTANT_TRAIT_TYPE));
        traits.replace(result);
        assertEqualsIgnoreOrder(traits.get(CONSTANT_TRAIT_TYPE),
                ImmutableList.of(
                        constantTrait("c3", BIGINT, 6L)));

        traits.add(constantTrait("c4", BIGINT, 4L));
        traits.replace(filter(traits.get(CONSTANT_TRAIT_TYPE), ImmutableSet.of(variable("c4", BIGINT))));
        assertEqualsIgnoreOrder(traits.get(CONSTANT_TRAIT_TYPE),
                ImmutableList.of(
                        constantTrait("c4", BIGINT, 4L)));
    }

    private static List<ConstantTrait> filter(List<ConstantTrait> constantTraits, Set<VariableReferenceExpression> variables)
    {
        return constantTraits.stream()
                .filter(constant -> variables.contains(constant.getVariable()))
                .collect(toImmutableList());
    }

    private List<ConstantTrait> transform(Map<VariableReferenceExpression, RowExpression> assignments, List<ConstantTrait> constantTraits)
    {
        return TraitUtils.transform(metadata, TEST_SESSION, assignments, constantTraits);
    }

    private RowExpression cast(RowExpression expression, Type toType)
    {
        FunctionHandle cast = metadata.getFunctionManager().lookupCast(CastType.CAST, expression.getType().getTypeSignature(), toType.getTypeSignature());
        return new CallExpression(cast, toType, ImmutableList.of(expression));
    }

    private RowExpression add(RowExpression left, RowExpression right)
    {
        FunctionHandle function = resolution.arithmeticFunction(ArithmeticBinaryExpression.Operator.ADD, left.getType(), right.getType());
        Type returnType = metadata.getType(function.getSignature().getReturnType());
        return new CallExpression(function, returnType, ImmutableList.of(left, right));
    }

    private RowExpression rand(long range)
    {
        FunctionHandle function = metadata.getFunctionManager().lookupFunction(QualifiedName.of("rand"), fromTypes(BIGINT));
        Type returnType = metadata.getType(function.getSignature().getReturnType());
        return new CallExpression(function, returnType, ImmutableList.of(constant(range, BIGINT)));
    }

    private RowExpression regexp_extract(String string, String pattern)
    {
        FunctionHandle function = metadata.getFunctionManager().lookupFunction(QualifiedName.of("regexp_extract"), fromTypes(VARCHAR, JONI_REGEXP));
        Type returnType = metadata.getType(function.getSignature().getReturnType());
        return new CallExpression(
                function,
                returnType,
                ImmutableList.of(
                        constant(Slices.utf8Slice(string), VARCHAR),
                        cast(constant(Slices.utf8Slice(pattern), VARCHAR), JONI_REGEXP)));
    }
}