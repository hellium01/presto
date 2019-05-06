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
package com.facebook.presto.trait.propagator;

import com.facebook.presto.Session;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Memo;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.trait.Trait;
import com.facebook.presto.trait.TraitSet;
import com.facebook.presto.trait.TraitType;
import com.google.common.collect.ImmutableSet;

import java.util.List;

import static com.facebook.presto.trait.BasicTraitSet.emptyTraitSet;
import static java.util.Objects.requireNonNull;

public class CachingTraitProvider
        implements TraitProvider
{
    private final TraitPropagator propagator;
    private final Memo memo;
    private final Session session;
    private final TypeProvider types;

    public CachingTraitProvider(TraitPropagator propagator, Memo memo, Session session, TypeProvider types)
    {
        this.propagator = requireNonNull(propagator, "propagator is null");
        this.memo = requireNonNull(memo, "memo is null");
        this.session = requireNonNull(session, "session is null");
        this.types = types;
    }

    @Override
    public TraitSet expectedOutput(PlanNode node)
    {
        return memo.getExpectedTraitSet(memo.getGroup(node)).orElse(emptyTraitSet());
    }

    @Override
    public TraitSet providedOutput(PlanNode node)
    {
        return memo.getProvidedTraitSet(memo.getGroup(node)).orElse(emptyTraitSet());
    }

    @Override
    public <T extends Trait> List<T> expectedOutput(PlanNode node, TraitType<T> type)
    {
        return expectedOutput(node).get(type);
    }

    @Override
    public <T extends Trait> List<T> providedOutput(PlanNode node, TraitType<T> type)
    {
        TraitSet cached = providedOutput(node);
        if (cached.listTraits().contains(type)) {
            return cached.get(type);
        }
        cached = propagator.pullUp(node, this, new TraitPropagationContext(session, types), ImmutableSet.of(type));
        memo.addProvidedTrait(memo.getGroup(node), cached);
        return cached.get(type);
    }
}
