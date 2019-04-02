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
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.relation.ConstantExpression;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

public class ConnectorRowRangeTrait
{
    private final Optional<ConnectorTableLayoutHandle> handle;
    private final Iterable<TupleDomain<ColumnHandle>> domains;
    private final Optional<List<ColumnHandle>> partitionColumns;
    private final Optional<Predicate<Map<ColumnHandle, ConstantExpression>>> predicateFunction;

    public ConnectorRowRangeTrait(
            Optional<ConnectorTableLayoutHandle> handle,
            Iterable<TupleDomain<ColumnHandle>> domains,
            Optional<List<ColumnHandle>> partitionColumns,
            Optional<Predicate<Map<ColumnHandle, ConstantExpression>>> predicate)
    {
        this.handle = handle;
        this.domains = domains;
        this.partitionColumns = partitionColumns;
        this.predicateFunction = predicate;
    }

    public Optional<ConnectorTableLayoutHandle> getHandle()
    {
        return handle;
    }

    public Iterable<TupleDomain<ColumnHandle>> getDomains()
    {
        return domains;
    }

    public TupleDomain<ColumnHandle> getDomain()
    {
        return StreamSupport.stream(domains.spliterator(), false)
                .reduce(TupleDomain.all(), (l, r) -> l.intersect(r));
    }

    public Optional<List<ColumnHandle>> getPartitionColumns()
    {
        return partitionColumns;
    }

    public Optional<Predicate<Map<ColumnHandle, ConstantExpression>>> getPredicateFunction()
    {
        return predicateFunction;
    }
}
