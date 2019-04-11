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

import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;

import java.util.List;

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

    public static CollationTrait unordered()
    {
        return new CollationTrait(ImmutableList.of(), ImmutableList.of());
    }
}
