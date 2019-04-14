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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

public class BasicTraitSet
        implements TraitSet
{
    private Map<TraitType<?>, List<Trait>> traits = new HashMap<>();

    public static BasicTraitSet emptyTraitSet()
    {
        return new BasicTraitSet();
    }

    @Override
    public TraitSet add(Trait trait)
    {
        TraitType<?> traitType = trait.getTraitType();
        traits.putIfAbsent(traitType, new ArrayList<>());
        traits.get(traitType).add(trait);
        return this;
    }

    @Override
    public TraitSet addAll(List<? extends Trait> traits)
    {
        if (traits.isEmpty()) {
            return this;
        }
        traits.forEach(trait -> add(trait));
        return this;
    }

    @Override
    public <T extends Trait> TraitSet replace(T trait)
    {
        TraitType<?> traitType = trait.getTraitType();
        traits.put(traitType, new ArrayList<>());
        traits.get(traitType).add(trait);
        return this;
    }

    @Override
    public <T extends Trait> TraitSet replace(List<T> traits)
    {
        checkArgument(!traits.isEmpty(), "cannot replace with empty list");
        TraitType<?> traitType = traits.get(0).getTraitType();
        this.traits.put(traitType, new ArrayList<>());
        this.traits.get(traitType).addAll(traits);
        return this;
    }

    @Override
    public <T extends Trait> Optional<T> getSingle(TraitType<T> traitType)
    {
        List<T> all = get(traitType);
        if (all.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(get(traitType).get(0));
    }

    @Override
    public <T extends Trait> List<T> get(TraitType<T> traitType)
    {
        List<T> result = (List<T>) traits.getOrDefault(traitType, Collections.emptyList());
        if (result.isEmpty()) {
            return result;
        }
        if (traitType.isMergable()) {
            return ImmutableList.of(traitType.merge(result));
        }
        if (!traitType.isAllowMulti()) {
            return ImmutableList.of(result.get(result.size() - 1));
        }
        return traitType.deduplicate(result);
    }

    @Override
    public <T extends Trait> boolean satisfies(T trait)
    {
        TraitType<?> type = trait.getTraitType();
        if (!traits.containsKey(type)) {
            return false;
        }
        List<T> current = this.get((TraitType<T>) type);
        return Lists.reverse(current).stream()
                .anyMatch(t -> t.satisfies(trait));
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
        return traits.keySet();
    }

    @Override
    public TraitSet clone()
    {
        return new LazyCopyTraitSet(this);
    }
}
