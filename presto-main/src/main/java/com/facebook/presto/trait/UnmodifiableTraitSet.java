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
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class UnmodifiableTraitSet
        implements TraitSet
{
    private final TraitSet delegate;

    public UnmodifiableTraitSet(TraitSet delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public TraitSet add(Trait trait)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TraitSet addAll(List<? extends Trait> traits)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends Trait> TraitSet replace(T trait)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends Trait> TraitSet replace(List<T> traits)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends Trait> Optional<T> getSingle(TraitType<T> traitType)
    {
        return delegate.getSingle(traitType);
    }

    @Override
    public <T extends Trait> List<T> get(TraitType<T> traitType)
    {
        return delegate.get(traitType);
    }

    @Override
    public <T extends Trait> boolean satisfies(T trait)
    {
        return delegate.satisfies(trait);
    }

    @Override
    public <T extends Trait> boolean satisfies(Collection<T> traits)
    {
        return delegate.satisfies(traits);
    }

    @Override
    public TraitSet merge(TraitSet traitSet)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<TraitType<?>> listTraits()
    {
        return delegate.listTraits();
    }

    @Override
    public TraitSet clone()
    {
        return new UnmodifiableTraitSet(delegate);
    }
}
