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
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.trait.traits.TraitSet;

import java.util.List;
import java.util.Optional;

public class ProjectTraitRule
        extends SimpleTraitRule<ProjectNode>
{
    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return Pattern.typeOf(ProjectNode.class);
    }

    @Override
    public Optional<TraitSet> pushDown(ProjectNode node, TraitSet parentTrait, Lookup lookup, Session session, TypeProvider types)
    {
        // TODO: rename traits based on assignment
        return super.pushDown(node, parentTrait, lookup, session, types);
    }

    @Override
    public Optional<TraitSet> pullUp(ProjectNode node, Lookup lookup, Session session, TypeProvider types, List<TraitSet> inputTraits)
    {
        return super.pullUp(node, lookup, session, types, inputTraits);
    }
}
