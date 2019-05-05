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

import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.trait.Trait;
import com.facebook.presto.trait.TraitSet;
import com.facebook.presto.trait.TraitType;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Multimaps.toMultimap;
import static java.util.function.Function.identity;

public class RuleBasedTraitPropagator
        implements TraitPropagator
{
    private final ListMultimap<TraitType<?>, PushDownRule<?, ?>> pushdownRulesByTraitType;
    private final ListMultimap<TraitType<?>, PullUpRule<?, ?>> pullupRulesByTraitType;

    public RuleBasedTraitPropagator(List<PushDownRule<?, ?>> pushDownRules, List<PullUpRule<?, ?>> pullUpRules)
    {
        this.pushdownRulesByTraitType = pushDownRules.stream()
                .collect(toMultimap(PushDownRule::getTraitType, identity(), ArrayListMultimap::create));
        this.pullupRulesByTraitType = pullUpRules.stream()
                .collect(toMultimap(PullUpRule::getTraitType, identity(), ArrayListMultimap::create));
    }

    @Override
    public TraitSet pullUp(PlanNode planNode, TraitProvider traitProvider, Context context, Set<TraitType> selectedTraitTypes)
    {
        TraitSet traitSet = traitProvider.providedOutput(planNode);
        checkArgument(planNode instanceof GroupReference, "planNode must be a groupReference");
        selectedTraitTypes.stream()
                .flatMap(traitType -> pullupRulesByTraitType.get(traitType).stream())
                .filter(rule -> rule.getPlanNodeType().isInstance(planNode))
                .map(rule -> pullUp(rule, planNode, traitProvider, context))
                .forEach(newTraitSet -> traitSet.merge(newTraitSet));
        return traitSet;
    }

    @Override
    public void pushDown(PlanNode planNode, TraitProvider traitProvider, Context context, Set<TraitType> selectedTraitTypes)
    {
        selectedTraitTypes.stream()
                .flatMap(traitType -> pushdownRulesByTraitType.get(traitType).stream())
                .filter(rule -> rule.getPlanNodeType().isInstance(planNode))
                .forEach(rule -> pushDown(rule, planNode, traitProvider, context));
    }

    private static <T extends Trait, P extends PlanNode> TraitSet pullUp(PullUpRule<T, P> rule, PlanNode planNode, TraitProvider traitProvider, Context context)
    {
        return rule.pullUp((P) planNode, traitProvider, context);
    }

    private static <T extends Trait, P extends PlanNode> void pushDown(PushDownRule<T, P> rule, PlanNode planNode, TraitProvider traitProvider, Context context)
    {
        rule.pushDown((P) planNode, traitProvider, context);
    }

    public interface PushDownRule<T extends Trait, P extends PlanNode>
    {
        TraitType<T> getTraitType();

        Class<P> getPlanNodeType();

        void pushDown(P planNode, TraitProvider traitProvider, Context context);
    }

    public interface PullUpRule<T extends Trait, P extends PlanNode>
    {
        TraitType<T> getTraitType();

        Class<P> getPlanNodeType();

        TraitSet pullUp(P planNode, TraitProvider traitProvider, Context context);
    }
}
