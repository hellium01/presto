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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.spi.SortingProperty;
import com.facebook.presto.spi.block.SortOrder;

import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;

public class ConnectorCollationTrait
        extends CollationTrait<ColumnHandle>
{
    public ConnectorCollationTrait(List<ColumnHandle> columns, Map<ColumnHandle, SortOrder> columnOrders)
    {
        super(columns, columnOrders);
    }

    private ConnectorCollationTrait(UnmodifiableLinkedHashMap<ColumnHandle, SortOrder> orders)
    {
        super(orders);
    }

    public List<ColumnHandle> getColumns()
    {
        return orders.getKeys();
    }

    public Map<ColumnHandle, SortOrder> getOrders()
    {
        return orders;
    }

    public boolean satisfies(Trait target)
    {
        if (!(target instanceof ConnectorCollationTrait)) {
            return false;
        }
        throw new UnsupportedOperationException();
    }

    public Trait getCoverage(Trait target)
    {
        throw new UnsupportedOperationException();
    }

    public List<SortingProperty<ColumnHandle>> toLocalProperties()
    {
        return unmodifiableList(orders.entrySet()
                .stream()
                .map(entry -> new SortingProperty<>(entry.getKey(), entry.getValue()))
                .collect(toList()));
    }

    public static ConnectorCollationTrait fromLocalProperties(List<LocalProperty<ColumnHandle>> localProperties)
    {
        UnmodifiableLinkedHashMap.Builder<ColumnHandle, SortOrder> result = new UnmodifiableLinkedHashMap.Builder<>();
        localProperties.stream()
                .filter(SortingProperty.class::isInstance)
                .map(SortingProperty.class::cast)
                .forEach(sort -> result.put((ColumnHandle) sort.getColumn(), sort.getOrder()));
        return new ConnectorCollationTrait(result.build());
    }
}
