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

import java.util.List;

public class TraitSet
{
    private List<Trait> traits;

    public TraitSet merge(TraitSet traitSet)
    {
        return emptySet();
    }

    public static TraitSet emptySet()
    {
        return new TraitSet();
    }

    public <T extends Trait> T getTrait(TraitType<T> traitType)
    {
        return null;
    }

    public <T extends Trait> void replaceTrait(TraitType<T> traitType, T trait)
    {
    }

    public TraitSet duplicate()
    {
        return new LazyCopyTraitSet();
    }
}
