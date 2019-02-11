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
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.relation.Aggregate;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ColumnReferenceExpression;
import com.facebook.presto.spi.relation.Filter;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.Project;
import com.facebook.presto.spi.relation.Relation;
import com.facebook.presto.spi.relation.RelationVisitor;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.TableScan;
import com.facebook.presto.spi.relation.UnaryNode;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.relational.RowExpressionToSqlTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toMap;
import static org.glassfish.jersey.internal.util.collection.ImmutableCollectors.toImmutableList;

public class PlanGenerator
{
    private final PlanNodeIdAllocator idAllocator;
    private final SymbolAllocator symbolAllocator;
    private final LiteralEncoder literalEncoder;
    private final FunctionRegistry functionRegistry;
    private final Metadata metadata;

    public PlanGenerator(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, LiteralEncoder literalEncoder, Metadata metadata)
    {
        this.idAllocator = idAllocator;
        this.symbolAllocator = symbolAllocator;
        this.literalEncoder = literalEncoder;
        this.functionRegistry = metadata.getFunctionRegistry();
        this.metadata = metadata;
    }

    public Optional<PlanNode> toPlan(Session session, ConnectorId connectorId, Relation relation, List<Symbol> outputSymbols)
    {
        return relation.accept(new PlanCreator(session, connectorId), new Context(outputSymbols));
    }

    private static class Context
    {

        private List<Symbol> outputChannels;

        public Context(List<Symbol> outputChannels)
        {
            this.outputChannels = ImmutableList.copyOf(outputChannels);
        }

        public List<Symbol> getOutput()
        {
            return outputChannels;
        }
    }

    private class PlanCreator
            extends RelationVisitor<Optional<PlanNode>, Context>
    {
        private final Session session;
        private final ConnectorId connectorId;

        public PlanCreator(Session session, ConnectorId connectorId)
        {
            this.session = session;
            this.connectorId = connectorId;
        }

        @Override
        protected Optional<PlanNode> visitProject(Project project, Context context)
        {
            List<Symbol> inputChannelNames = getInputChannels(project);
            Optional<PlanNode> child = project.getSource().accept(this, new Context(inputChannelNames));

            if (child.isPresent()) {
                Assignments.Builder builder = Assignments.builder();
                List<Expression> assignments = project.getOutput().stream()
                        .map(rowExpression -> translate(rowExpression, inputChannelNames))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(toImmutableList());
                if (assignments.size() != project.getOutput().size()) {
                    return Optional.empty();
                }
                IntStream.range(0, assignments.size()).boxed()
                        .forEach(i -> builder.put(context.getOutput().get(i), assignments.get(i)));
                return Optional.of(new ProjectNode(idAllocator.getNextId(), child.get(), builder.build()));
            }
            return Optional.empty();
        }

        private List<Symbol> getInputChannels(UnaryNode node)
        {
            // First if expression is InputReference use output name
            return IntStream.range(0, node.getSource().getOutput().size())
                    .boxed()
                    .map(i ->
                            symbolAllocator.newSymbol(
                                    getNameHint(node.getSource().getOutput().get(i)),
                                    node.getSource().getOutput().get(i).getType()))
                    .collect(toImmutableList());
        }

        private Optional<Expression> translate(RowExpression rowExpression, List<Symbol> inputChannelNames)
        {
            return RowExpressionToSqlTranslator.translate(rowExpression, inputChannelNames, ImmutableMap.of(), literalEncoder, functionRegistry);
        }

        @Override
        protected Optional<PlanNode> visitFilter(Filter filter, Context context)
        {
            List<Symbol> inputChannelNames = getInputChannels(filter);
            Optional<PlanNode> child = filter.getSource().accept(this, context);
            if (child.isPresent()) {
                Optional<Expression> predicate = translate(filter.getPredicate(), inputChannelNames);
                if (predicate.isPresent()) {
                    return Optional.of(new FilterNode(idAllocator.getNextId(), child.get(), predicate.get()));
                }
            }
            return Optional.empty();
        }

        @Override
        protected Optional<PlanNode> visitAggregate(Aggregate aggregate, Context context)
        {
            // TODO make the output ... etc. correct
            List<Symbol> outputChannelNames = context.getOutput();
            List<Symbol> inputChannelNames = getInputChannels(aggregate);
            Optional<PlanNode> child = aggregate.getSource().accept(this, new Context(inputChannelNames));
            if (!child.isPresent()) {
                return Optional.empty();
            }
            int numGroups = aggregate.getGroups().size();
            AggregationNode.GroupingSetDescriptor groupingSetDescriptor;
            Optional<Symbol> groupIdSymbol = Optional.empty();
            if (aggregate.getGroupSets().size() > 1) {
                groupIdSymbol = Optional.of(symbolAllocator.newSymbol("groupId", BIGINT));
                Map<Integer, Symbol> groupingChannelName = aggregate.getGroupSets().stream()
                        .flatMap(List::stream)
                        .collect(Collectors.toSet())
                        .stream()
                        .collect(
                                toMap(i -> i,
                                        i -> symbolAllocator.newSymbol(inputChannelNames.get(i).getName(), aggregate.getSource().getOutput().get(i).getType(), "gid")));
                List<List<Symbol>> groupingSets = aggregate.getGroupSets()
                        .stream()
                        .map(set -> set.stream().map(i -> groupingChannelName.get(i)).collect(toImmutableList()))
                        .collect(toImmutableList());
                List<Symbol> aggregateColumns = IntStream.range(0, child.get().getOutputSymbols().size())
                        .boxed()
                        .filter(i -> !groupingChannelName.containsKey(i))
                        .map(i -> inputChannelNames.get(i))
                        .collect(Collectors.toList());
                Map<Symbol, Symbol> groupingColumnNames = groupingChannelName.entrySet()
                        .stream()
                        .collect(toMap(entry -> entry.getValue(), entry -> inputChannelNames.get(entry.getKey())));
                child = Optional.of(
                        new GroupIdNode(
                                idAllocator.getNextId(),
                                child.get(),
                                groupingSets,
                                groupingColumnNames,
                                aggregateColumns, groupIdSymbol.get()));
                groupingSetDescriptor = new AggregationNode.GroupingSetDescriptor(
                        Stream.concat(
                                aggregate.getGroups().stream()
                                        .filter(InputReferenceExpression.class::isInstance)
                                        .map(InputReferenceExpression.class::cast)
                                        .map(InputReferenceExpression::getField)
                                        .map(i -> groupingChannelName.get(i)),
                                Stream.of(groupIdSymbol.get())
                        ).collect(toImmutableList()),
                        aggregate.getGroupSets().size(),
                        ImmutableSet.of());
            }
            else {
                groupingSetDescriptor = new AggregationNode.GroupingSetDescriptor(
                        aggregate.getGroups().stream()
                                .filter(InputReferenceExpression.class::isInstance)
                                .map(InputReferenceExpression.class::cast)
                                .map(InputReferenceExpression::getField)
                                .map(i -> inputChannelNames.get(i))
                                .collect(toImmutableList()),
                        aggregate.getGroupSets().size(),
                        ImmutableSet.of());
            }
            Map<Symbol, AggregationNode.Aggregation> aggregations = aggregate.getAggregations()
                    .stream()
                    .filter(CallExpression.class::isInstance)
                    .map(CallExpression.class::cast)
                    .collect(
                            toMap(
                                    rowExpression -> symbolAllocator.newSymbol(rowExpression.getSignature().getName(), rowExpression.getType()),
                                    rowExpression -> new AggregationNode.Aggregation(
                                            (FunctionCall) translate(rowExpression, inputChannelNames).get(),
                                            rowExpression.getSignature(),
                                            Optional.empty())));
            return Optional.of(
                    new AggregationNode(
                            idAllocator.getNextId(),
                            child.get(),
                            aggregations,
                            groupingSetDescriptor,
                            ImmutableList.of(),
                            AggregationNode.Step.SINGLE,
                            Optional.empty(),
                            groupIdSymbol));
        }

        @Override
        protected Optional<PlanNode> visitTableScan(TableScan tableScan, Context context)
        {
            //TODO clean up the code here
            List<Symbol> outputChannelNames = context.getOutput();
            List<ColumnHandle> columnHandles = tableScan.getOutput().stream()
                    .filter(outputColumn -> outputColumn instanceof ColumnReferenceExpression)
                    .map(outputColumn -> ((ColumnReferenceExpression) outputColumn).getColumnHandle())
                    .collect(Collectors.toList());
            checkArgument(columnHandles.size() == tableScan.getOutput().size(), "tableScan must contains all columnHandle");
            Map<Symbol, ColumnHandle> columnHandleMap = IntStream.range(0, columnHandles.size())
                    .boxed()
                    .collect(toMap(i -> outputChannelNames.get(i), i -> columnHandles.get(i)));

            Optional<TableLayoutHandle> tableLayoutHandle = Optional.empty();
            if (tableScan.getConnectorTableLayoutHandle().isPresent()) {
                tableLayoutHandle = Optional.of(
                        new TableLayoutHandle(
                                connectorId,
                                metadata.getTransactionHandle(session, connectorId),
                                tableScan.getConnectorTableLayoutHandle().get()));
            }
            return Optional.of(new TableScanNode(
                    idAllocator.getNextId(),
                    new TableHandle(connectorId, tableScan.getTableHandle()),
                    outputChannelNames,
                    columnHandleMap,
                    tableLayoutHandle,
                    TupleDomain.all(),
                    TupleDomain.all()));
        }

        private Optional<Expression> rewriteRowExpression(RowExpression rowExpression)
        {
            return Optional.empty();
        }

        private String getNameHint(RowExpression rowExpression)
        {
            //TODO name hit for NamedColumnHandle (which provides getName)
            if (rowExpression instanceof CallExpression) {
                return ((CallExpression) rowExpression).getSignature().getName();
            }
            else if (rowExpression instanceof ColumnReferenceExpression) {
                return "col";
            }
            return "expr";
        }
    }
}
