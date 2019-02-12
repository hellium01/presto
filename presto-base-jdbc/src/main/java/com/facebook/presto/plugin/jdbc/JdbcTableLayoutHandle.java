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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class JdbcTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final JdbcTableHandle table;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final Optional<String> tableFilter;
    private final List<ColumnHandle> groupingKeys;

    @JsonCreator
    public JdbcTableLayoutHandle(
            @JsonProperty("table") JdbcTableHandle table,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> domain,
            @JsonProperty("tableFilter") Optional<String> tableFilter,
            @JsonProperty("groupingKeys") List<ColumnHandle> groupingKeys)
    {
        this.table = requireNonNull(table, "table is null");
        this.tupleDomain = requireNonNull(domain, "tupleDomain is null");
        this.tableFilter = requireNonNull(tableFilter, "tableFilter is null");
        this.groupingKeys = ImmutableList.copyOf(requireNonNull(groupingKeys, "groupingKeys is null"));
    }

    public JdbcTableLayoutHandle(JdbcTableHandle table, TupleDomain<ColumnHandle> domain)
    {
        this(table, domain, Optional.empty(), ImmutableList.of());
    }

    public JdbcTableLayoutHandle withGroupingKeys(List<ColumnHandle> groupingKeys)
    {
        return new JdbcTableLayoutHandle(this.table, this.tupleDomain, this.tableFilter, groupingKeys);
    }

    public JdbcTableLayoutHandle withTableFilter(String tableFilter)
    {
        return new JdbcTableLayoutHandle(this.table, this.tupleDomain, Optional.of(tableFilter), this.groupingKeys);
    }

    @JsonProperty
    public JdbcTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }

    @JsonProperty
    public Optional<String> getTableFilter()
    {
        return tableFilter;
    }

    @JsonProperty
    public List<ColumnHandle> getGroupingKeys()
    {
        return groupingKeys;
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
        JdbcTableLayoutHandle that = (JdbcTableLayoutHandle) o;
        return Objects.equals(table, that.table) &&
                Objects.equals(tupleDomain, that.tupleDomain) &&
                Objects.equals(tableFilter, that.tableFilter) &&
                Objects.equals(groupingKeys, that.groupingKeys);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, tupleDomain, tableFilter, groupingKeys);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table.toString())
                .add("tupleDomain", tupleDomain.toString())
                .add("tableFilter", tableFilter.toString())
                .add("groupingKeys", groupingKeys.toString())
                .toString();
    }
}
