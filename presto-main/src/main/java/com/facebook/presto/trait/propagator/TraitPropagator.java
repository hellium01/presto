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
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.trait.TraitSet;
import com.facebook.presto.trait.TraitType;

import java.util.Set;

public interface TraitPropagator
{
    /**
     * Pull up provided output from planNode's input. Can be only a subset of traits and the result is merged with
     * existing value.
     *
     * @param planNode
     * @param traitProvider
     * @param context
     * @param selectedTraitTypes
     * @return
     */
    TraitSet pullUp(PlanNode planNode, TraitProvider traitProvider, Context context, Set<TraitType> selectedTraitTypes);

    /**
     * Push down preferred input from planNode's output. Will update in place in trait returned by traitProvider.
     *
     * @param planNode
     * @param traitProvider
     * @param context
     * @param selectedTraitTypes
     * @return
     */
    void pushDown(PlanNode planNode, TraitProvider traitProvider, Context context, Set<TraitType> selectedTraitTypes);

    interface Context
    {
        Session getSession();

        TypeProvider getTypes();
    }
}
