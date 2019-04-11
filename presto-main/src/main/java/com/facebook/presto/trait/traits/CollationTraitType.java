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
import java.util.ListIterator;

import static com.google.common.base.Preconditions.checkArgument;

public class CollationTraitType
        extends TraitType<CollationTrait>
{
    public final static CollationTraitType COLLATION_TRAIT_TYPE = new CollationTraitType();

    public CollationTraitType()
    {
        super(true, false);
    }

    @Override
    public CollationTrait merge(List<CollationTrait> traits)
    {
        checkArgument(!traits.isEmpty(), "cannot merge empty list");
        ListIterator<CollationTrait> it = traits.listIterator(traits.size());
        CollationTrait mostSpecific = it.previous();
        while(it.hasPrevious()) {
            CollationTrait current = it.previous();
            if (current.satisfies(mostSpecific)) {
                mostSpecific = current;
            } else {
                break;
            }
        }
        return mostSpecific;
    }
}
