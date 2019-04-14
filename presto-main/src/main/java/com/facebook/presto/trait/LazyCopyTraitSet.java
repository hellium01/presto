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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class LazyCopyTraitSet
        implements TraitSet
{
    private final TraitSet original;
    private final TraitSet current = BasicTraitSet.emptyTraitSet();

    public LazyCopyTraitSet(TraitSet original)
    {
        requireNonNull(original, "original is null");
        this.original = new UnmodifiableTraitSet(original);
    }

    @Override
    public TraitSet add(Trait trait)
    {
        TraitType type = trait.getTraitType();
        if (current.listTraits().contains(type)) {
            current.addAll(Lists.reverse(original.get(type)));
        }
        current.add(trait);
        return this;
    }

    @Override
    public TraitSet addAll(List<? extends Trait> traits)
    {
        traits.stream()
                .forEach(this::add);
        return this;
    }

    @Override
    public <T extends Trait> TraitSet replace(T trait)
    {
        current.replace(trait);
        return this;
    }

    @Override
    public <T extends Trait> TraitSet replace(List<T> traits)
    {
        current.replace(traits);
        return this;
    }

    @Override
    public <T extends Trait> Optional<T> getSingle(TraitType<T> traitType)
    {
        if (current.listTraits().contains(traitType)) {
            return current.getSingle(traitType);
        }
        return original.getSingle(traitType);
    }

    @Override
    public <T extends Trait> List<T> get(TraitType<T> traitType)
    {
        if (current.listTraits().contains(traitType)) {
            return current.get(traitType);
        }
        return original.get(traitType);
    }

    @Override
    public <T extends Trait> boolean satisfies(T trait)
    {
        if (current.listTraits().contains(trait.getTraitType())) {
            return current.satisfies(trait);
        }
        return original.satisfies(trait);
    }

    @Override
    public <T extends Trait> boolean satisfies(Collection<T> traits)
    {
        return traits.stream()
                .allMatch(this::satisfies);
    }

    @Override
    public Set<TraitType<?>> listTraits()
    {
        return Sets.union(original.listTraits(), current.listTraits());
    }
}
