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

import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.trait.TraitSet;
import com.facebook.presto.trait.TraitType;
import com.facebook.presto.trait.propagator.RuleBasedTraitPropagator;
import com.facebook.presto.trait.propagator.TraitPropagator;
import com.facebook.presto.trait.propagator.TraitProvider;

import static com.facebook.presto.trait.BasicTraitSet.emptyTraitSet;
import static com.facebook.presto.trait.traits.DataSourceTraitType.DATA_SOURCE_TRAIT;

public class DataSourceTraitPullupRule
        implements RuleBasedTraitPropagator.PullUpRule<DataSourceTrait, PlanNode>
{
    @Override
    public TraitType<DataSourceTrait> getTraitType()
    {
        return DATA_SOURCE_TRAIT;
    }

    @Override
    public Class<PlanNode> getPlanNodeType()
    {
        return PlanNode.class;
    }

    @Override
    public TraitSet pullUp(PlanNode planNode, TraitProvider traitProvider, TraitPropagator.Context context)
    {
        TraitSet traits = emptyTraitSet();
        planNode.getSources()
                .stream()
                .forEach(input -> traits.addAll(traitProvider.providedOutput(input, DATA_SOURCE_TRAIT)));
        return traits;
    }
}
