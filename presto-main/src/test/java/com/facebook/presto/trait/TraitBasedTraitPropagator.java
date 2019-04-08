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
package com.facebook.presto.trait;

import com.facebook.presto.Session;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.trait.traits.Trait;
import com.facebook.presto.trait.traits.TraitSet;

import java.util.List;
import java.util.Optional;

public class TraitBasedTraitPropagator<T extends Trait>
        implements TraitSetPropagator
{
    @Override
    public TraitSet pushDownTraitSet(Session session, TypeProvider types, PlanNode planNode, TraitSet inputTraitSet)
    {
        return null;
    }

    @Override
    public TraitSet pullUpTraitSet(Session session, TypeProvider types, PlanNode planNode, List<PlanNode> inputs, List<TraitSet> inputTraits)
    {
        return null;
    }

    public interface Rule<T extends Trait>
    {
        Pattern<T> getPattern();

        Optional<TraitSet> pushDown(PlanNode node, T trait, Lookup lookup, Session session, TypeProvider types);

        Optional<TraitSet> pullUp(PlanNode node, Lookup lookup, Session session, TypeProvider types, List<T> inputTraits);
    }
}
