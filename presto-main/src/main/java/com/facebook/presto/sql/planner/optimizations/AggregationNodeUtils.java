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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.isExpression;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class AggregationNodeUtils
{
    private AggregationNodeUtils() {}

    public static AggregationNode.Aggregation count(FunctionManager functionManager)
    {
        return new AggregationNode.Aggregation(
                new CallExpression("count",
                        new FunctionResolution(functionManager).countFunction(),
                        BIGINT,
                        ImmutableList.of()),
                Optional.empty(),
                Optional.empty(),
                false,
                Optional.empty());
    }

    public static Set<VariableReferenceExpression> extractAggregationUniqueVariables(AggregationNode.Aggregation aggregation, TypeProvider types)
    {
        // types will be no longer needed once everything is RowExpression.
        ImmutableSet.Builder<VariableReferenceExpression> builder = ImmutableSet.builder();
        aggregation.getArguments().forEach(argument -> builder.addAll(extractAll(argument, types)));
        aggregation.getFilter().ifPresent(filter -> builder.addAll(extractAll(filter, types)));
        aggregation.getOrderBy().ifPresent(orderingScheme -> builder.addAll(orderingScheme.getOrderByVariables()));
        return builder.build();
    }

    private static List<VariableReferenceExpression> extractAll(RowExpression expression, TypeProvider types)
    {
        if (isExpression(expression)) {
            return VariablesExtractor.extractAll(castToExpression(expression), types);
        }
        return VariablesExtractor.extractAll(expression)
                .stream()
                .collect(toImmutableList());
    }

    public static AggregationNode.GroupingSetDescriptor globalAggregation()
    {
        return singleGroupingSet(ImmutableList.of());
    }

    public static AggregationNode.GroupingSetDescriptor singleGroupingSet(List<VariableReferenceExpression> groupingKeys)
    {
        Set<Integer> globalGroupingSets;
        if (groupingKeys.isEmpty()) {
            globalGroupingSets = ImmutableSet.of(0);
        }
        else {
            globalGroupingSets = ImmutableSet.of();
        }

        return new AggregationNode.GroupingSetDescriptor(groupingKeys, 1, globalGroupingSets);
    }

    public static AggregationNode.GroupingSetDescriptor groupingSets(List<VariableReferenceExpression> groupingKeys, int groupingSetCount, Set<Integer> globalGroupingSets)
    {
        return new AggregationNode.GroupingSetDescriptor(groupingKeys, groupingSetCount, globalGroupingSets);
    }

    public static boolean isDecomposable(AggregationNode node, FunctionManager functionManager)
    {
        boolean hasOrderBy = node.getAggregations().values().stream()
                .map(AggregationNode.Aggregation::getOrderBy)
                .anyMatch(Optional::isPresent);

        boolean hasDistinct = node.getAggregations().values().stream()
                .anyMatch(AggregationNode.Aggregation::isDistinct);

        boolean decomposableFunctions = node.getAggregations().values().stream()
                .map(AggregationNode.Aggregation::getFunctionHandle)
                .map(functionManager::getAggregateFunctionImplementation)
                .allMatch(InternalAggregationFunction::isDecomposable);

        return !hasOrderBy && !hasDistinct && decomposableFunctions;
    }

    public static boolean hasSingleNodeExecutionPreference(AggregationNode node, FunctionManager functionManager)
    {
        // There are two kinds of aggregations the have single node execution preference:
        //
        // 1. aggregations with only empty grouping sets like
        //
        // SELECT count(*) FROM lineitem;
        //
        // there is no need for distributed aggregation. Single node FINAL aggregation will suffice,
        // since all input have to be aggregated into one line output.
        //
        // 2. aggregations that must produce default output and are not decomposable, we can not distribute them.
        return (hasEmptyGroupingSet(node) && !hasNonEmptyGroupingSet(node)) || (hasDefaultOutput(node) && !isDecomposable(node, functionManager));
    }

    /**
     * @return whether this node should produce default output in case of no input pages.
     * For example for query:
     * <p>
     * SELECT count(*) FROM nation WHERE nationkey < 0
     * <p>
     * A default output of "0" is expected to be produced by FINAL aggregation operator.
     */
    public static boolean hasDefaultOutput(AggregationNode node)
    {
        return hasEmptyGroupingSet(node) && (node.getStep().isOutputPartial() || node.getStep().equals(SINGLE));
    }

    public static boolean hasEmptyGroupingSet(AggregationNode node)
    {
        return !node.getGroupingSets().getGlobalGroupingSets().isEmpty();
    }

    public static boolean hasNonEmptyGroupingSet(AggregationNode node)
    {
        return node.getGroupingSets().getGroupingSetCount() > node.getGroupingSets().getGlobalGroupingSets().size();
    }

    public static boolean hasOrderings(AggregationNode node)
    {
        return node.getAggregations().values().stream()
                .map(AggregationNode.Aggregation::getOrderBy)
                .anyMatch(Optional::isPresent);
    }

    public static boolean isStreamable(AggregationNode node)
    {
        return !node.getPreGroupedVariables().isEmpty() && node.getGroupingSets().getGroupingSetCount() == 1 && node.getGroupingSets().getGlobalGroupingSets().isEmpty();
    }
}
