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
import com.facebook.presto.matching.pattern.TypeOfPattern;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.trait.traits.TraitSet;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import java.lang.reflect.Modifier;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Multimaps.toMultimap;

public class NodeBasedTraitPropagator
        implements TraitSetPropagator
{
    private final ListMultimap<Class<?>, NodeBasedTraitPropagator.Rule<?>> rulesByRootType;

    public NodeBasedTraitPropagator(List<NodeBasedTraitPropagator.Rule<?>> rules)
    {
        this.rulesByRootType = rules.stream()
                .peek(rule -> {
                    checkArgument(rule.getPattern() instanceof TypeOfPattern, "Rule pattern must be TypeOfPattern");
                    Class<?> expectedClass = ((TypeOfPattern<?>) rule.getPattern()).expectedClass();
                    checkArgument(!expectedClass.isInterface() && !Modifier.isAbstract(expectedClass.getModifiers()), "Rule must be registered on a concrete class");
                })
                .collect(toMultimap(
                        rule -> ((TypeOfPattern<?>) rule.getPattern()).expectedClass(),
                        rule -> rule,
                        ArrayListMultimap::create));
    }

    @Override
    public TraitSet pushDownTraitSet(Session session, TypeProvider types, PlanNode node, TraitSet inputTraitSet)
    {
        Iterator<Rule<?>> ruleIterator = getCandidates(node).iterator();
        while (ruleIterator.hasNext()) {
            Rule<?> rule = ruleIterator.next();
            Optional<TraitSet> traitSet = ((Rule) rule).pushDown(node, inputTraitSet, Lookup.noLookup(), session, types);
            if (traitSet.isPresent()) {
                return traitSet.get();
            }
        }
        return TraitSet.emptySet();
    }

    @Override
    public TraitSet pullUpTraitSet(Session session, TypeProvider types, PlanNode node, List<PlanNode> inputs, List<TraitSet> inputTraits)
    {
        return TraitSet.emptySet();
    }

    private Stream<NodeBasedTraitPropagator.Rule<?>> getCandidates(PlanNode node)
    {
        for (Class<?> superclass = node.getClass().getSuperclass(); superclass != null; superclass = superclass.getSuperclass()) {
            // This is important because rule ordering, given in the constructor, is significant.
            // We can't check this fully in the constructor, since abstract class may lack `abstract` modifier.
            checkState(rulesByRootType.get(superclass).isEmpty(), "Cannot maintain rule order because there is rule registered for %s", superclass);
        }
        return rulesByRootType.get(node.getClass()).stream();
    }

    public interface Rule<T extends PlanNode>
    {
        Pattern<T> getPattern();

        Optional<TraitSet> pushDown(T node, TraitSet parentTrait, Lookup lookup, Session session, TypeProvider types);

        Optional<TraitSet> pullUp(T node,  Lookup lookup, Session session, TypeProvider types, List<TraitSet> inputTraits);
    }
}
