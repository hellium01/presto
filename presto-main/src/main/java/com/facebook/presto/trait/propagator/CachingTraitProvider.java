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
import com.facebook.presto.sql.planner.iterative.Memo;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.trait.BasicTraitSet;
import com.facebook.presto.trait.Trait;
import com.facebook.presto.trait.TraitSet;
import com.facebook.presto.trait.TraitType;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class CachingTraitProvider
        implements TraitProvider
{
    private final TraitPropagator propagator;
    private final Optional<Memo> memo;
    private final Session session;

    public CachingTraitProvider(TraitPropagator propagator, Optional<Memo> memo, Session session)
    {
        this.propagator = requireNonNull(propagator, "propagator is null");
        this.memo = requireNonNull(memo, "memo is null");
        this.session = requireNonNull(session, "session is null");
    }

    @Override
    public TraitSet preferredOutput(PlanNode node)
    {
        return BasicTraitSet.emptyTraitSet();
    }

    @Override
    public TraitSet providedOutput(PlanNode node)
    {
        return BasicTraitSet.emptyTraitSet();
    }

    @Override
    public <T extends Trait> List<T> preferredOutput(PlanNode node, TraitType<T> type)
    {
        return null;
    }

    @Override
    public <T extends Trait> List<T> providedOutput(PlanNode node, TraitType<T> type)
    {
        return null;
    }

    private Optional<TraitSet> getCachedTraitSet(PlanNode node)
    {
        return Optional.empty();
    }
}
