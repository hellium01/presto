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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.trait.traits.TraitSet;

import java.util.List;
import java.util.Optional;

public class TableScanTraitRule
        extends SimpleTraitRule<TableScanNode>
{
    private final Metadata metadata;

    public TableScanTraitRule(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public Pattern<TableScanNode> getPattern()
    {
        return null;
    }

    @Override
    public Optional<TraitSet> pushDown(TableScanNode node, TraitSet parentTrait, Lookup lookup, Session session, TypeProvider types)
    {
        // leafNode cannot push trait into connector directly
        return Optional.empty();
    }

    @Override
    public Optional<TraitSet> pullUp(TableScanNode node, Lookup lookup, Session session, TypeProvider types, List<TraitSet> inputTraits)
    {
        // metadata.getLayout(session, node.getTable());
        // metadata.get
        return Optional.empty();
    }
}
