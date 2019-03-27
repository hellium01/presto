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
package com.facebook.presto.metadata;

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorLayoutProperties;
import com.facebook.presto.spi.DiscretePredicates;
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class TableProperties
{
    private final TableHandle handle;
    private final ConnectorLayoutProperties layout;

    public TableProperties(TableHandle handle, ConnectorLayoutProperties layout)
    {
        requireNonNull(handle, "handle is null");
        requireNonNull(layout, "layout is null");

        this.handle = handle;
        this.layout = layout;
    }

    public ConnectorId getConnectorId()
    {
        return handle.getConnectorId();
    }

    public Optional<List<ColumnHandle>> getColumns()
    {
        return layout.getColumns();
    }

    public TupleDomain<ColumnHandle> getPredicate()
    {
        return layout.getPredicate();
    }

    public List<LocalProperty<ColumnHandle>> getLocalProperties()
    {
        return layout.getLocalProperties();
    }

    public TableHandle getTableHandle()
    {
        return handle;
    }

    public Optional<TablePartitioning> getTablePartitioning()
    {
        return layout.getTablePartitioning()
                .map(nodePartitioning -> new TablePartitioning(
                        new PartitioningHandle(
                                Optional.of(handle.getConnectorId()),
                                Optional.of(handle.getTransaction()),
                                nodePartitioning.getPartitioningHandle()),
                        nodePartitioning.getPartitioningColumns()));
    }

    public Optional<Set<ColumnHandle>> getStreamPartitioningColumns()
    {
        return layout.getStreamPartitioningColumns();
    }

    public Optional<DiscretePredicates> getDiscretePredicates()
    {
        return layout.getDiscretePredicates();
    }

    public static TableProperties fromConnectorLayoutProperties(TableHandle table, ConnectorLayoutProperties layoutProperties)
    {
        return new TableProperties(table, layoutProperties);
    }

    public static class TablePartitioning
    {
        private final PartitioningHandle partitioningHandle;
        private final List<ColumnHandle> partitioningColumns;

        public TablePartitioning(PartitioningHandle partitioningHandle, List<ColumnHandle> partitioningColumns)
        {
            this.partitioningHandle = requireNonNull(partitioningHandle, "partitioningHandle is null");
            this.partitioningColumns = ImmutableList.copyOf(requireNonNull(partitioningColumns, "partitioningColumns is null"));
        }

        public PartitioningHandle getPartitioningHandle()
        {
            return partitioningHandle;
        }

        public List<ColumnHandle> getPartitioningColumns()
        {
            return partitioningColumns;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TablePartitioning that = (TablePartitioning) o;
            return Objects.equals(partitioningHandle, that.partitioningHandle) &&
                    Objects.equals(partitioningColumns, that.partitioningColumns);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(partitioningHandle, partitioningColumns);
        }
    }
}
