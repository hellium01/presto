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

import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.trait.TraitType;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import static com.facebook.presto.trait.traits.TraitUtils.constantTrait;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class ConstantTraitType
        extends TraitType<ConstantTrait>
{
    public final static ConstantTraitType CONSTANT_TRAIT_TYPE = new ConstantTraitType();

    public ConstantTraitType()
    {
        super(false, true);
    }

    @Override
    public List<ConstantTrait> deduplicate(List<ConstantTrait> traits)
    {
        ListIterator<ConstantTrait> it = traits.listIterator(traits.size());
        Map<VariableReferenceExpression, ConstantExpression> visited = new LinkedHashMap<>();
        while (it.hasPrevious()) {
            ConstantTrait value = it.previous();
            visited.putIfAbsent(value.getVariable(), value.getValue());
        }
        return visited.entrySet()
                .stream()
                .map(entry -> constantTrait(entry.getKey().getName(), entry.getKey().getType(), entry.getValue().getValue()))
                .collect(toImmutableList());
    }
}
