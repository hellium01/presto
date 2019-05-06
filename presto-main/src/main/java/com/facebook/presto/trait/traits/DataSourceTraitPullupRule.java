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
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.trait.TraitType;
import com.facebook.presto.trait.propagator.RuleBasedTraitPropagator;
import com.facebook.presto.trait.propagator.TraitPropagator;
import com.facebook.presto.trait.propagator.TraitProvider;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.trait.traits.DataSourceTraitType.DATA_SOURCE_TRAIT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

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
    public List<DataSourceTrait> pullUp(PlanNode planNode, TraitProvider traitProvider, TraitPropagator.Context context)
    {
        return new Visitor().visitPlan(planNode, traitProvider);
    }

    private class Visitor
            extends PlanVisitor<List<DataSourceTrait>, TraitProvider>
    {
        @Override
        protected List<DataSourceTrait> visitPlan(PlanNode node, TraitProvider context)
        {
            checkArgument(node.getSources().size() > 0, format("%s must has at least one source Node", node));
            if (node.getSources().size() == 1) {
                return context.providedOutput(node.getSources().get(0), DATA_SOURCE_TRAIT);
            }

            return node.getSources()
                    .stream()
                    .map(source -> context.providedOutput(source, DATA_SOURCE_TRAIT))
                    .flatMap(List::stream)
                    .collect(toImmutableList());
        }

        @Override
        public List<DataSourceTrait> visitTableScan(TableScanNode node, TraitProvider context)
        {
            return ImmutableList.of(new DataSourceTrait(node.getTable().getConnectorId()));
        }

        @Override
        public List<DataSourceTrait> visitValues(ValuesNode node, TraitProvider context)
        {
            return ImmutableList.of();
        }
    }
}
