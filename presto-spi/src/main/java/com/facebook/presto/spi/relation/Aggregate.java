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
package com.facebook.presto.spi.relation;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Aggregate
        extends UnaryNode
{
    private final List<RowExpression> aggregations;
    private final List<RowExpression> groups;
    private final Optional<RowExpression> groupId;
    private final List<List<Integer>> groupSets;
    private final Relation source;

    public Aggregate(List<RowExpression> aggregations, List<RowExpression> groups, Optional<RowExpression> groupId, List<List<Integer>> groupSets, Relation source)
    {
        this.aggregations = aggregations;
        this.groups = groups;
        this.groupId = groupId;
        this.groupSets = groupSets;
        this.source = source;
    }

    public List<RowExpression> getAggregations()
    {
        return aggregations;
    }

    public List<RowExpression> getGroups()
    {
        return groups;
    }

    public Optional<RowExpression> getGroupId()
    {
        return groupId;
    }

    public List<List<Integer>> getGroupSets()
    {
        return groupSets;
    }

    @Override
    public Relation getSource()
    {
        return source;
    }

    @Override
    public List<RowExpression> getOutput()
    {
        return Stream.concat(
                Stream.concat(groups.stream(), aggregations.stream()),
                groupId.map(Stream::of).orElseGet(Stream::empty))
                .collect(Collectors.toList());
    }

    @Override
    public <R, C> R accept(RelationVisitor<R, C> visitor, C context)
    {
        return visitor.visitAggregate(this, context);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Aggregate)) {
            return false;
        }
        Aggregate aggregate = (Aggregate) o;
        return Objects.equals(aggregations, aggregate.aggregations) &&
                Objects.equals(groups, aggregate.groups) &&
                Objects.equals(source, aggregate.source);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(aggregations, groups, source);
    }
}
