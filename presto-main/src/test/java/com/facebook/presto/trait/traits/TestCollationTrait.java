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

import com.facebook.presto.trait.TraitSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.relational.Expressions.variable;
import static com.facebook.presto.trait.traits.CollationTraitType.COLLATION_TRAIT_TYPE;
import static com.facebook.presto.trait.traits.Order.ASC_NULLS_FIRST;
import static com.facebook.presto.trait.traits.Order.ASC_NULLS_LAST;
import static com.facebook.presto.trait.traits.TraitUtils.orderTrait;
import static com.facebook.presto.trait.traits.TraitUtils.unordered;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestCollationTrait
{
    @Test
    public void testMultiPassOrdering()
    {
        TraitSet traits = TraitSet.emptyTraitSet();
        CollationTrait t1 = orderTrait(
                ImmutableList.of(variable("c1", BIGINT), variable("c2", BIGINT)),
                ImmutableList.of(ASC_NULLS_FIRST, ASC_NULLS_FIRST));
        assertTrue(t1.satisfies(t1));
        traits.add(t1);
        CollationTrait t2 = orderTrait(
                ImmutableList.of(variable("c1", BIGINT), variable("c2", BIGINT), variable("c3", VARCHAR)),
                ImmutableList.of(ASC_NULLS_FIRST, ASC_NULLS_FIRST, ASC_NULLS_LAST));
        traits.add(t2);
        assertTrue(t2.satisfies(t1));
        assertEquals(traits.getSingle(COLLATION_TRAIT_TYPE), t2);
        CollationTrait t3 = orderTrait(
                ImmutableList.of(variable("c1", BIGINT), variable("c2", BIGINT), variable("c3", VARCHAR)),
                ImmutableList.of(ASC_NULLS_LAST, ASC_NULLS_FIRST, ASC_NULLS_LAST));
        traits.add(t3);
        assertFalse(t3.satisfies(t2));
        CollationTrait t4 = orderTrait(
                ImmutableList.of(variable("c1", BIGINT)),
                ImmutableList.of(ASC_NULLS_LAST));
        traits.add(t4);
        assertTrue(t3.satisfies(t4));
        assertEquals(traits.getSingle(COLLATION_TRAIT_TYPE), t3);
        CollationTrait t5 = unordered();
        traits.add(t5);
        assertFalse(t5.satisfies(t5));
        assertFalse(t5.satisfies(t4));
        assertFalse(t4.satisfies(t5));
        assertEquals(traits.getSingle(COLLATION_TRAIT_TYPE), t5);
    }

    @Test
    public void testTransform()
    {
        CollationTrait t1 = orderTrait(
                ImmutableList.of(variable("c1", BIGINT), variable("c2", BIGINT)),
                ImmutableList.of(ASC_NULLS_FIRST, ASC_NULLS_FIRST));
        assertEquals(t1.transform(ImmutableMap.of(variable("c1", BIGINT), variable("c1_1", BIGINT)), ImmutableSet.of(variable("c2", BIGINT))), unordered());
        CollationTrait t2 = orderTrait(
                ImmutableList.of(variable("c1_1", BIGINT)),
                ImmutableList.of(ASC_NULLS_FIRST));
        assertEquals(t1.transform(ImmutableMap.of(variable("c1", BIGINT), variable("c1_1", BIGINT)), ImmutableSet.of(variable("c1_1", BIGINT))), t2);
        CollationTrait t3 = orderTrait(
                ImmutableList.of(variable("c1", BIGINT)),
                ImmutableList.of(ASC_NULLS_FIRST));
        assertEquals(t1.transform(ImmutableMap.of(variable("c2", BIGINT), variable("c2_1", BIGINT)), ImmutableSet.of(variable("c1", BIGINT))), t3);
    }
}