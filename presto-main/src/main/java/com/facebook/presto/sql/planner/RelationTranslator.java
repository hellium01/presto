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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.spi.relation.ColumnReferenceExpression;
import com.facebook.presto.spi.relation.Filter;
import com.facebook.presto.spi.relation.Project;
import com.facebook.presto.spi.relation.Relation;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.TableScan;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static java.util.stream.Collectors.toMap;

public class RelationTranslator
{
    private final Metadata metadata;
    private final TypeProvider types;
    private final Session session;
    private final SqlParser parser;
    private final Lookup lookup;

    public RelationTranslator(Metadata metadata, TypeProvider types, Session session, SqlParser parser, Lookup lookup)
    {
        this.metadata = metadata;
        this.types = types;
        this.session = session;
        this.parser = parser;
        this.lookup = lookup;
    }

    public Optional<Relation> translate(PlanNode plan)
    {
        return plan.accept(new PlanRewriter(), null);
    }

    private class PlanRewriter
            extends PlanVisitor<Optional<Relation>, Void>
    {
        @Override
        protected Optional<Relation> visitPlan(PlanNode node, Void context)
        {
            return Optional.empty();
        }

        @Override
        public Optional<Relation> visitAggregation(AggregationNode node, Void context)
        {
            return super.visitAggregation(node, context);
        }

        @Override
        public Optional<Relation> visitFilter(FilterNode node, Void context)
        {
            Map<NodeRef<Expression>, Type> expressionTypes =
                    getExpressionTypes(session, metadata, parser, types, node.getPredicate(), ImmutableList.of(), WarningCollector.NOOP, false);
            RowExpression predicate = SqlToRowExpressionTranslator.translate(node.getPredicate(), SCALAR, expressionTypes,
                    ImmutableMap.of(), getSymbol(node.getSource().getOutputSymbols()), metadata.getFunctionRegistry(), metadata.getTypeManager(), session, false);
            Optional<Relation> child = lookup.resolveGroup(node.getSource()).findAny().flatMap(planNode -> planNode.accept(this, null));
            if (child.isPresent()) {
                return Optional.of(new Filter(predicate, child.get()));
            }
            return Optional.empty();
        }

        private Map<String, Integer> getSymbol(List<Symbol> symbols)
        {
            return IntStream.range(0, symbols.size())
                    .boxed()
                    .collect(toMap(i -> symbols.get(i).getName(), i -> i));
        }

        @Override
        public Optional<Relation> visitProject(ProjectNode node, Void context)
        {
            Assignments assignments = node.getAssignments();
            Map<NodeRef<Expression>, Type> expressionTypes =
                    getExpressionTypes(session, metadata, parser, types, assignments.getExpressions(), ImmutableList.of(), WarningCollector.NOOP, false);
            Map<String, Integer> inputs = getSymbol(node.getSource().getOutputSymbols());
            Map<Symbol, RowExpression> assignmentRowExpressions = assignments.getMap()
                    .entrySet()
                    .stream()
                    .collect(toMap(entry -> entry.getKey(), entry -> {
                        RowExpression rowExpression = SqlToRowExpressionTranslator.translate(
                                entry.getValue(), SCALAR, expressionTypes,
                                ImmutableMap.of(), inputs, metadata.getFunctionRegistry(), metadata.getTypeManager(), session, false);
                        return rowExpression;
                    }));

            Optional<Relation> child = lookup.resolveGroup(node.getSource()).findAny().flatMap(planNode -> planNode.accept(this, context));
            if (!child.isPresent()) {
                return Optional.empty();
            }
            return Optional.of(new Project(node.getOutputSymbols().stream().map(symbol -> assignmentRowExpressions.get(symbol)).collect(Collectors.toList()), child.get()));
        }

        @Override
        public Optional<Relation> visitTableScan(TableScanNode node, Void context)
        {
            return Optional.of(new TableScan(
                    node.getTable().getConnectorHandle(),
                    node.getOutputSymbols().stream()
                            .map(symbol -> new ColumnReferenceExpression(node.getAssignments().get(symbol), types.get(symbol)))
                            .collect(Collectors.toList()),
                    node.getLayout().map(TableLayoutHandle::getTransactionHandle),
                    node.getLayout().map(TableLayoutHandle::getConnectorHandle)));
        }
    }
}
