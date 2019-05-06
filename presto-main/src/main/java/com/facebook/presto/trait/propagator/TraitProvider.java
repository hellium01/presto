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

public interface TraitProvider
{
    TraitSet expectedOutput(PlanNode node);

    TraitSet providedOutput(PlanNode node);

    default <T extends Trait> List<T> expectedOutput(PlanNode node, TraitType<T> type)
    {
        return expectedOutput(node).get(type);
    }

    default <T extends Trait> List<T> providedOutput(PlanNode node, TraitType<T> type)
    {
        return providedOutput(node).get(type);
    }
}
