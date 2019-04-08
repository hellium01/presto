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
package com.facebook.presto.trait.rules;

import com.facebook.presto.Session;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.trait.traits.RowRangeTrait;
import com.facebook.presto.trait.traits.TraitSet;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.trait.traits.RowRangeTraitType.ROW_FILTER;

public class FilterTraitRule
        extends SimpleTraitRule<FilterNode>
{
    @Override
    public Pattern<FilterNode> getPattern()
    {
        return Pattern.typeOf(FilterNode.class);
    }

    @Override
    public Optional<TraitSet> pushDown(FilterNode node, TraitSet parentTrait, Lookup lookup, Session session, TypeProvider types)
    {
        TraitSet localTrait = parentTrait.duplicate();
        RowRangeTrait trait = parentTrait.getTrait(ROW_FILTER);
        pushDownRowRangeTrait(trait, toRowExpression(node.getPredicate())).ifPresent(result -> localTrait.replaceTrait(ROW_FILTER, result));
        return Optional.of(localTrait);
    }

    @Override
    public Optional<TraitSet> pullUp(FilterNode node, Lookup lookup, Session session, TypeProvider types, List<TraitSet> inputTraits)
    {
        return Optional.empty();
    }

    private RowExpression toRowExpression(Expression expression)
    {
        return null;
    }

    private Optional<RowRangeTrait> pushDownRowRangeTrait(RowRangeTrait trait, RowExpression predicate)
    {
        // combine
        // extract conjuncts
        // filter out conjunct that does not contains output symbols
        return Optional.empty();
    }
}
