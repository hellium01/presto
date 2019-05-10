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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

public class SingleTraitSet<T extends Trait>
        implements TraitSet
{
    private final TraitType<T> traitType;
    private final List<T> traits;

    private SingleTraitSet(TraitType<T> traitType, List<T> traits)
    {
        this.traitType = traitType;
        this.traits = traits;
    }

    public static <U extends Trait> SingleTraitSet<U> of(TraitType<U> traitType)
    {
        return new SingleTraitSet<>(traitType, Collections.emptyList());
    }

    @Override
    public TraitSet add(Trait trait)
    {
        checkArgument(trait.getTraitType().equals(traitType));
        traits.add((T) trait);
        return this;
    }

    @Override
    public TraitSet addAll(List<? extends Trait> traits)
    {
        traits.forEach(this::add);
        return this;
    }

    @Override
    public <T extends Trait> TraitSet replace(T trait)
    {
        traits.clear();
        add(trait);
        return this;
    }

    @Override
    public <T extends Trait> TraitSet replace(List<T> traits)
    {
        traits.clear();
        addAll(traits);
        return this;
    }

    @Override
    public <T extends Trait> Optional<T> getSingle(TraitType<T> traitType)
    {
        return Optional.empty();
    }

    @Override
    public <T extends Trait> List<T> get(TraitType<T> traitType)
    {
        return null;
    }

    @Override
    public <T extends Trait> boolean satisfies(T trait)
    {
        return false;
    }

    @Override
    public <T extends Trait> boolean satisfies(Collection<T> traits)
    {
        return false;
    }

    @Override
    public Set<TraitType<?>> listTraits()
    {
        return null;
    }
}
