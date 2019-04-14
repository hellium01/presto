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

import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface TraitSet
{
    TraitSet add(Trait trait);

    TraitSet addAll(List<? extends Trait> traits);

    <T extends Trait> BasicTraitSet replace(T trait);

    <T extends Trait> TraitSet replace(List<T> traits);

    <T extends Trait> T getSingle(TraitType<T> traitType);

    <T extends Trait> List<T> get(TraitType<T> traitType);

    <T extends Trait> boolean satisfies(T trait);

    <T extends Trait> boolean satisfies(Collection<T> traits);

    TraitSet merge(TraitSet traitSet);

    Set<TraitType<?>> listTraits();
}
