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

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.planner.optimizations.RowExpressionCanonicalizer;
import com.facebook.presto.trait.Trait;
import com.facebook.presto.trait.TraitType;

import java.util.List;

import static com.facebook.presto.sql.relational.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.trait.traits.RowFilterTraitType.ROW_FILTER;

public class RowFilterTrait
        implements Trait
{
    private final RowExpressionCanonicalizer canonicalizer = new RowExpressionCanonicalizer(MetadataManager.createTestMetadataManager().getFunctionManager());
    private RowExpression predicate;

    public RowFilterTrait(RowExpression predicate)
    {
        this.predicate = predicate;
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

    public boolean satisfies(RowFilterTrait trait)
    {
        // check if boolean rowExpression satisfies other will be
        // optimize
        List<RowExpression> source = extractConjuncts(canonicalizer.canonicalize(this.predicate));
        List<RowExpression> target = extractConjuncts(canonicalizer.canonicalize(trait.getPredicate()));
        return target.stream().allMatch(source::contains);
    }

    public RowExpression getPredicate()
    {
        return predicate;
    }
}
