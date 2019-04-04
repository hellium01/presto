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
package com.facebook.presto.sql.planner.plan;

<<<<<<< HEAD:presto-main/src/main/java/com/facebook/presto/sql/planner/plan/EnforceSingleRowNode.java
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
=======
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.planner.Symbol;
>>>>>>> Replace FilterNode::Expression with RowExpression:presto-main/src/main/java/com/facebook/presto/sql/planner/plan/FilterNode.java
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static java.util.Objects.requireNonNull;

@Immutable
public class EnforceSingleRowNode
        extends InternalPlanNode
{
    private final PlanNode source;
<<<<<<< HEAD:presto-main/src/main/java/com/facebook/presto/sql/planner/plan/EnforceSingleRowNode.java

    @JsonCreator
    public EnforceSingleRowNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source)
    {
        super(id);

        this.source = requireNonNull(source, "source is null");
=======
    private final RowExpression predicate;

    @JsonCreator
    public FilterNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("predicate") RowExpression predicate)
    {
        super(id);

        this.source = source;
        this.predicate = predicate;
    }

    @JsonProperty("predicate")
    public RowExpression getPredicate()
    {
        return predicate;
>>>>>>> Replace FilterNode::Expression with RowExpression:presto-main/src/main/java/com/facebook/presto/sql/planner/plan/FilterNode.java
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return source.getOutputVariables();
    }

    @JsonProperty("source")
    public PlanNode getSource()
    {
        return source;
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitEnforceSingleRow(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new EnforceSingleRowNode(getId(), Iterables.getOnlyElement(newChildren));
    }
}
