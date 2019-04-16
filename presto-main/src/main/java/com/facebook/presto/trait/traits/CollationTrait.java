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

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.trait.Trait;
import com.facebook.presto.trait.TraitType;
import com.facebook.presto.trait.utils.UnmodifiableLinkedHashMap;
import com.google.common.collect.Streams;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.trait.traits.CollationTraitType.COLLATION_TRAIT_TYPE;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class CollationTrait
        implements Trait
{
    private final UnmodifiableLinkedHashMap<VariableReferenceExpression, Order> orderMap;

    public CollationTrait(List<VariableReferenceExpression> variables, List<Order> orders)
    {
        requireNonNull(variables, "variables is null");
        requireNonNull(orders, "orders is null");
        checkArgument(variables.size() == orders.size(), "variables must have same size as orders");
        this.orderMap = new UnmodifiableLinkedHashMap.Builder<>(variables, orders).build();
    }

    public CollationTrait(UnmodifiableLinkedHashMap<VariableReferenceExpression, Order> orders)
    {
        requireNonNull(orders, "orders is null");
        this.orderMap = orders;
    }

    public List<VariableReferenceExpression> getVariables()
    {
        return orderMap.getKeys();
    }

    public Map<VariableReferenceExpression, Order> getOrders()
    {
        return orderMap;
    }

    public CollationTrait transform(Map<VariableReferenceExpression, VariableReferenceExpression> assignments, Set<VariableReferenceExpression> allowedVariables)
    {
        UnmodifiableLinkedHashMap.Builder<VariableReferenceExpression, Order> builder = new UnmodifiableLinkedHashMap.Builder<>();
        for (Map.Entry<VariableReferenceExpression, Order> entry : orderMap.entrySet()) {
            VariableReferenceExpression translated = assignments.getOrDefault(entry.getKey(), entry.getKey());
            if (!allowedVariables.contains(translated)) {
                break;
            }
            builder.put(translated, entry.getValue());
        }
        return new CollationTrait(builder.build());
    }

    @Override
    public boolean satisfies(Trait other)
    {
        if (other.getTraitType() != this.getTraitType()) {
            return false;
        }
        return satisfies((CollationTrait) other);
    }

    public boolean satisfies(CollationTrait other)
    {
        // unordered cannot be satisfied by an ordered trait or satisfy an ordered trait
        if (orderMap.isEmpty() || other.orderMap.isEmpty()) {
            return false;
        }
        Iterator<Map.Entry<VariableReferenceExpression, Order>> it1 = orderMap.entrySet().iterator();
        Iterator<Map.Entry<VariableReferenceExpression, Order>> it2 = other.orderMap.entrySet().iterator();
        while (it1.hasNext() && it2.hasNext()) {
            Map.Entry<VariableReferenceExpression, Order> entry1 = it1.next();
            Map.Entry<VariableReferenceExpression, Order> entry2 = it2.next();
            if (!entry1.getKey().equals(entry2.getKey())) {
                return false;
            }
            if (!entry1.getValue().equals(entry2.getValue())) {
                return false;
            }
        }
        if (it1.hasNext()) {
            return true;
        }
        return !it2.hasNext();
    }

    @Override
    public boolean isEnforced()
    {
        return orderMap.values().stream().allMatch(Order::isStrict);
    }

    @Override
    public TraitType<?> getTraitType()
    {
        return COLLATION_TRAIT_TYPE;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CollationTrait)) {
            return false;
        }
        CollationTrait trait = (CollationTrait) o;
        return Streams.zip(orderMap.entrySet().stream(), trait.orderMap.entrySet().stream(),
                (l, r) -> l.getKey().equals(r.getKey()) && l.getValue().equals(r.getValue()))
                .allMatch(Boolean::booleanValue);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(orderMap);
    }

    @Override
    public String toString()
    {
        return "CollationTrait{" +
                orderMap.entrySet()
                        .stream().map(entry -> format("%s%s", entry.getKey(), toOrderSymbol(entry.getValue())))
                        .collect(Collectors.joining(",")) +
                '}';
    }

    private String toOrderSymbol(Order order)
    {
        String s1;
        String s2;
        if (order.isFixed()) {
            s1 = "\u2299";
        }
        else if (order.isAscending()) {
            if (order.isStrict()) {
                s1 = "\u2191";
            }
            else {
                s1 = "\u21e1";
            }
        }
        else {
            if (order.isStrict()) {
                s1 = "\u2193";
            }
            else {
                s1 = "\u21e3";
            }
        }

        if (order.isNullsFirst()) {
            s2 = "\u2190";
        }
        else {
            s2 = "\u2192";
        }
        return s1 + s2;
    }
}
