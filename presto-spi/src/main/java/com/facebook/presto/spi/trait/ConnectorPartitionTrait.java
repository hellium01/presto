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
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorSplitManager;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * This trait describes partitioning of data in a table into split group or single split.
 * A table is partitioned on columns {C1,...,Cn} if rows with the same values
 * on the respective columns are always in the same split group.
 * <p>
 * This class was previously name ConnectorNodePartitioning.
 * The new semantics is more flexible.
 * It enables expression of more sophisticated table organization.
 * Node partitioning is a form of partitioning between split groups,
 * where each split group contains all splits on a {@link Node}.
 * <p>
 * Unless the connector declares itself as supporting grouped scheduling in
 * {@link ConnectorSplitManager}, Presto engine treats all splits on a node
 * as a single split group.
 * As a result, the connector does not need to provide additional guarantee
 * as a result of this change for previously-declared node partitioning.
 * Therefore, this change in SPI is backward compatible.
 * <p>
 * For now, all splits in each split group must be assigned the same {@link Node}
 * by {@link ConnectorNodePartitioningProvider}.
 * With future changes to the engine, connectors will no longer be required
 * to declare a mapping from split groups to nodes.
 * Artificially requiring such a mapping regardless of whether the engine can
 * take advantage of the TablePartitioning negatively affects performance.
 */
public class ConnectorPartitionTrait
{
    private final ConnectorPartitioningHandle partitioningHandle;
    private final List<ColumnHandle> partitioningColumns;
    private final Optional<Set<ColumnHandle>> streamingPartitioningColumns;

    public ConnectorPartitionTrait(ConnectorPartitioningHandle partitioningHandle, List<ColumnHandle> partitioningColumns, Optional<Set<ColumnHandle>> streamingPartitioningColumns)
    {
        this.partitioningHandle = partitioningHandle;
        this.partitioningColumns = partitioningColumns;
        this.streamingPartitioningColumns = streamingPartitioningColumns;
    }

    public ConnectorPartitioningHandle getPartitioningHandle()
    {
        return partitioningHandle;
    }

    /**
     * The partitioning of the table across the worker nodes.
     * <p>
     * If the table is node partitioned, the connector guarantees that each combination of values for
     * the distributed columns will be contained within a single worker.
     */
    public List<ColumnHandle> getPartitioningColumns()
    {
        return partitioningColumns;
    }

    /**
     * The partitioning for the table streams.
     * If empty, the table layout is partitioned arbitrarily.
     * Otherwise, table steams are partitioned on the given set of columns (or unpartitioned, if the set is empty)
     * <p>
     * If the table is partitioned, the connector guarantees that each combination of values for
     * the partition columns will be contained within a single split (i.e., partitions cannot
     * straddle multiple splits)
     */
    public Optional<Set<ColumnHandle>> getStreamingPartitioningColumns()
    {
        return streamingPartitioningColumns;
    }
}
