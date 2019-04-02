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
import com.facebook.presto.spi.GroupingProperty;
import com.facebook.presto.spi.LocalProperty;

import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toList;

public class ConnectorGroupingTrait
{
    private final Set<ColumnHandle> columns;

    public ConnectorGroupingTrait(Set<ColumnHandle> columns)
    {
        this.columns = columns;
    }

    public static List<ConnectorGroupingTrait> fromLocalProperties(List<LocalProperty<ColumnHandle>> localProperties)
    {
        return localProperties.stream()
                .filter(GroupingProperty.class::isInstance)
                .map(GroupingProperty.class::cast)
                .map(grouping -> new ConnectorGroupingTrait(grouping.getColumns()))
                .collect(toList());
    }

    public LocalProperty<ColumnHandle> toLocalProperty()
    {
        return new GroupingProperty<>(columns);
    }
}
