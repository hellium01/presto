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
import com.facebook.presto.trait.Trait;
import com.facebook.presto.trait.TraitType;

import static com.facebook.presto.trait.traits.RowFilterTraitType.ROW_FILTER;

public class RowFilterTrait
        implements Trait
{
    private final PredicateComparator comparator;
    private final RowExpression predicate;

    public RowFilterTrait(RowExpression predicate, PredicateComparator comparator)
    {
        this.predicate = predicate;
        this.comparator = comparator;
    }

    public RowFilterTrait newPredicate(RowExpression predicate)
    {
        return new RowFilterTrait(predicate, comparator);
    }

    @Override
    public TraitType<?> getTraitType()
    {
        return ROW_FILTER;
    }

    @Override
    public boolean satisfies(Trait trait)
    {
        if (trait.getTraitType() != this.getTraitType()) {
            return false;
        }
        return satisfies((RowFilterTrait) trait);
    }

    @Override
    public boolean isEnforced()
    {
        return true;
    }

    public boolean satisfies(RowFilterTrait trait)
    {
        // check if current predicate is more specific than other (what is true in this will be always true in other).
        return comparator.checkLeftSatisfiesRight(this.getPredicate(), trait.getPredicate());
    }

    public RowExpression getPredicate()
    {
        return predicate;
    }
}
