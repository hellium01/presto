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

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.relation.Aggregate;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ColumnReferenceExpression;
import com.facebook.presto.spi.relation.Filter;
import com.facebook.presto.spi.relation.Project;
import com.facebook.presto.spi.relation.Relation;
import com.facebook.presto.spi.relation.RelationVisitor;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.TableScan;
import com.facebook.presto.spi.relation.UnaryNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.relational.RowExpressionToSqlTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toMap;
import static org.glassfish.jersey.internal.util.collection.ImmutableCollectors.toImmutableList;

public class PlanGenerator
{
    private final ConnectorId connectorId;
    private final PlanNodeIdAllocator idAllocator;
    private final SymbolAllocator symbolAllocator;
    private final LiteralEncoder literalEncoder;
    private final FunctionRegistry functionRegistry;

    public PlanGenerator(ConnectorId connectorId, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, LiteralEncoder literalEncoder, FunctionRegistry functionRegistry)
    {
        this.connectorId = connectorId;
        this.idAllocator = idAllocator;
        this.symbolAllocator = symbolAllocator;
        this.literalEncoder = literalEncoder;
        this.functionRegistry = functionRegistry;
    }

    public Optional<PlanNode> toPlan(Relation relation, List<Symbol> outputSymbols)
    {
        return relation.accept(new PlanCreator(), new Context(outputSymbols));
    }

    private static class Context
    {
        Map<Integer, String> outputChannels;

        public Context(Map<Integer, String> outputChannels)
        {
            this.outputChannels = outputChannels;
        }

        public Context(List<Symbol> outputChannels)
        {
            this.outputChannels = IntStream.range(0, outputChannels.size())
                    .boxed()
                    .collect(toMap(i -> i, i -> outputChannels.get(i).getName()));
        }

        public Map<Integer, String> getOutput()
        {
            return outputChannels;
        }
    }

    private class PlanCreator
            extends RelationVisitor<Optional<PlanNode>, Context>
    {
        @Override
        protected Optional<PlanNode> visitProject(Project project, Context context)
        {
            Map<Integer, String> inputChannelNames = getInputChannels(project);
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
                        .forEach(i -> builder.put(new Symbol(context.getOutput().get(i)), assignments.get(i)));
                return Optional.of(new ProjectNode(idAllocator.getNextId(), child.get(), builder.build()));
            }
            return Optional.empty();
        }

        private Map<Integer, String> getInputChannels(UnaryNode node)
        {
            return IntStream.range(0, node.getSource().getOutput().size())
                    .boxed()
                    .collect(toMap(i -> i, i -> symbolAllocator.newSymbol(
                            getNameHint(node.getSource().getOutput().get(i)),
                            node.getSource().getOutput().get(i).getType()).getName()));
        }

        private Optional<Expression> translate(RowExpression rowExpression, Map<Integer, String> inputChannelNames)
        {
            return RowExpressionToSqlTranslator.translate(rowExpression, inputChannelNames, ImmutableMap.of(), literalEncoder, functionRegistry);
        }

        @Override
        protected Optional<PlanNode> visitFilter(Filter filter, Context context)
        {
            Map<Integer, String> inputChannelNames = getInputChannels(filter);
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
            return Optional.empty();
        }

        @Override
        protected Optional<PlanNode> visitTableScan(TableScan tableScan, Context context)
        {
            List<ColumnHandle> columnHandles = tableScan.getOutput().stream()
                    .filter(outputColumn -> outputColumn instanceof ColumnReferenceExpression)
                    .map(outputColumn -> ((ColumnReferenceExpression) outputColumn).getColumnHandle())
                    .collect(Collectors.toList());
            checkArgument(columnHandles.size() == tableScan.getOutput().size(), "tableScan must contains all columnHandle");
            List<Symbol> outputColumnNames = tableScan.getOutput().stream()
                    .map(rowExpression -> symbolAllocator.newSymbol(getNameHint(rowExpression), rowExpression.getType()))
                    .collect(toImmutableList());
            Map<Symbol, ColumnHandle> columnHandleMap = IntStream.range(0, columnHandles.size())
                    .boxed()
                    .collect(toMap(i -> outputColumnNames.get(i), i -> columnHandles.get(i)));
            return Optional.of(new TableScanNode(
                    idAllocator.getNextId(),
                    new TableHandle(connectorId, tableScan.getTableHandle()),
                    outputColumnNames,
                    columnHandleMap));
        }

        private Optional<Expression> rewriteRowExpression(RowExpression rowExpression)
        {
            return Optional.empty();
        }

        private String getNameHint(RowExpression rowExpression)
        {
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
