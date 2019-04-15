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

import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.trait.Trait;
import com.facebook.presto.trait.TraitSet;
import com.facebook.presto.trait.TraitType;

import java.util.List;
import java.util.Set;

public class RuleBasedTraitPropagator
        implements TraitPropagator
{
    private final List<PushdownRule<?, ?>> rules;

    public RuleBasedTraitPropagator(List<PushdownRule<?, ?>> rules)
    {
        this.rules = rules;
    }

    public <T extends Trait, P extends PlanNode> void bind(TraitType<T> traitType, Class<P> planNodeType, PushdownRule<T, P> rule)
    {
    }

    @Override
    public TraitSet pullUp(PlanNode planNode, TraitProvider traitProvider, Context context, Set<TraitType> selectedTraitTypes)
    {
        return null;
    }

    @Override
    public void pushDown(PlanNode planNode, TraitProvider traitProvider, Context context, Set<TraitType> selectedTraitTypes)
    {

    }

    public interface PushdownRule<T extends Trait, P extends PlanNode>
    {
        TraitType<T> getTraitType();

        TraitSet pullUp(P planNode, TraitProvider traitProvider, Context context);
    }

    public interface PullupRule<T extends Trait, P extends PlanNode>
    {
        TraitType<T> getTraitType();

        TraitSet pullUp(P planNode, TraitProvider traitProvider, Context context);
    }
}
