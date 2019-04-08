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
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.trait.NodeBasedTraitPropagator;
import com.facebook.presto.trait.traits.TraitSet;

import java.util.List;
import java.util.Optional;

public abstract class SimpleTraitRule<T extends PlanNode>
        implements NodeBasedTraitPropagator.Rule<T>
{
    @Override
    public Optional<TraitSet> pushDown(T node, TraitSet parentTrait, Lookup lookup, Session session, TypeProvider types)
    {
        return Optional.of(parentTrait);
    }

    @Override
    public Optional<TraitSet> pullUp(T node, Lookup lookup, Session session, TypeProvider types, List<TraitSet> inputTraits)
    {
        return Optional.of(inputTraits.stream().reduce(TraitSet.emptySet(), (traitSet, traitSet2) -> traitSet.merge(traitSet2)));
    }
}
