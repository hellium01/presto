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

import com.facebook.presto.trait.TraitType;

import java.util.List;

import static com.facebook.presto.sql.relational.LogicalRowExpressions.and;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class RowFilterTraitType
        extends TraitType<RowFilterTrait>
{
    public static final RowFilterTraitType ROW_FILTER = new RowFilterTraitType();

    public RowFilterTraitType()
    {
        super(true, false);
    }

    @Override
    public RowFilterTrait merge(List<RowFilterTrait> traits)
    {
        return new RowFilterTrait(and(traits.stream().map(RowFilterTrait::getPredicate).collect(toImmutableList())));
    }
}
