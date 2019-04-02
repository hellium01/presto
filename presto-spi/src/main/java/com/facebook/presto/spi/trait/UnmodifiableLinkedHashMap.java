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
package com.facebook.presto.spi.trait;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * It only be built by builder and cannot be modified. The builder is not thread safe and will evict previous value once build() is called.
 *
 * @param <K> key of the delegatedMap
 * @param <V> value of the delegatedMap
 */
public class UnmodifiableLinkedHashMap<K, V>
        implements Map<K, V>
{
    private final Map<K, V> delegatedMap;

    private UnmodifiableLinkedHashMap(Map<K, V> delegatedMap)
    {
        this.delegatedMap = unmodifiableMap(delegatedMap);
    }

    public List<K> getKeys()
    {
        return unmodifiableList(delegatedMap.keySet().stream().collect(toList()));
    }

    @Override
    public int size()
    {
        return delegatedMap.size();
    }

    @Override
    public boolean isEmpty()
    {
        return delegatedMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key)
    {
        return delegatedMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value)
    {
        return delegatedMap.containsValue(value);
    }

    @Override
    public V get(Object key)
    {
        return delegatedMap.get(key);
    }

    @Override
    public V put(K key, V value)
    {
        throw new UnsupportedOperationException(format("Map %s is not modifiable", delegatedMap));
    }

    @Override
    public V remove(Object key)
    {
        throw new UnsupportedOperationException(format("Map %s is not modifiable", delegatedMap));
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m)
    {
        throw new UnsupportedOperationException(format("Map %s is not modifiable", delegatedMap));
    }

    @Override
    public void clear()
    {
        throw new UnsupportedOperationException(format("Map %s is not modifiable", delegatedMap));
    }

    @Override
    public Set<K> keySet()
    {
        return delegatedMap.keySet();
    }

    @Override
    public Collection<V> values()
    {
        return delegatedMap.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet()
    {
        return delegatedMap.entrySet();
    }

    public static class Builder<K, V>
    {
        private Map<K, V> cache = new LinkedHashMap<>();

        public Builder()
        {
        }

        public Builder(Map<? extends K, ? extends V> map)
        {
            putAll(map);
        }

        public <KK extends K, VV extends V> Builder(List<KK> keys, Map<KK, VV> map)
        {
            putAllInOrder(keys, map);
        }

        public Builder put(K key, V value)
        {
            if (cache == null) {
                cache = new LinkedHashMap<>();
            }
            cache.put(key, value);
            return this;
        }

        public Builder putAll(Map<? extends K, ? extends V> map)
        {
            if (cache == null) {
                cache = new LinkedHashMap<>();
            }
            cache.putAll(map);
            return this;
        }

        public <KK extends K, VV extends V> Builder putAllInOrder(List<KK> keys, Map<KK, VV> map)
        {
            if (cache == null) {
                cache = new LinkedHashMap<>();
            }
            keys.stream()
                    .forEach(key -> cache.put(key, map.get(key)));

            return this;
        }

        public UnmodifiableLinkedHashMap<K, V> build()
        {
            requireNonNull(cache, "builder is not correctly initialized");
            Map<K, V> built = cache;
            cache = null;
            return new UnmodifiableLinkedHashMap<>(built);
        }
    }
}
