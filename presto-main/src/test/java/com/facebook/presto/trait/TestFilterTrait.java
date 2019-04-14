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

package com.facebook.presto.trait;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.optimizations.RowExpressionCanonicalizer;
import com.facebook.presto.sql.relational.DeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.sql.relational.StandardFunctionResolution;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.trait.traits.PredicateComparator;
import com.facebook.presto.trait.traits.RowFilterTrait;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.SymbolsExtractor.extractAll;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.variable;
import static com.facebook.presto.sql.relational.LogicalRowExpressions.and;
import static com.facebook.presto.sql.relational.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.sql.relational.RowExpressionUtils.inlineExpressions;
import static com.facebook.presto.trait.BasicTraitSet.emptyTraitSet;
import static com.facebook.presto.trait.traits.RowFilterTraitType.ROW_FILTER;
import static org.testng.Assert.assertTrue;

public class TestFilterTrait
{
    private StandardFunctionResolution resolution;
    private Metadata metadata;
    private RowExpressionCanonicalizer canonicalizer;
    private DeterminismEvaluator determinismEvaluator;

    @BeforeMethod

    public void setUp()
    {
        metadata = createTestMetadataManager();
        resolution = new StandardFunctionResolution(metadata.getFunctionManager());
        canonicalizer = new RowExpressionCanonicalizer(metadata.getFunctionManager());
        determinismEvaluator = new DeterminismEvaluator(metadata.getFunctionManager());
    }

    @Test
    public void testFilterSatisfies()
    {
        assertSatisfies(
                filterTrait(
                        and(
                                gt(variable("c1", BIGINT), constant(1L, BIGINT)),
                                eq(variable("c2", BIGINT), constant(2L, BIGINT)))),

                filterTrait(
                        and(
                                gt(variable("c1", BIGINT), constant(1L, BIGINT)),
                                eq(variable("c2", BIGINT), constant(2L, BIGINT)))));

        assertSatisfies(
                filterTrait(
                        and(
                                gt(variable("c1", BIGINT), constant(1L, BIGINT)),
                                eq(variable("c2", BIGINT), constant(2L, BIGINT)))),

                filterTrait(
                        and(
                                eq(variable("c2", BIGINT), constant(2L, BIGINT)),
                                gt(variable("c1", BIGINT), constant(1L, BIGINT)))));
        assertSatisfies(
                filterTrait(
                        and(
                                gt(variable("c1", BIGINT), constant(1L, BIGINT)),
                                eq(variable("c2", BIGINT), constant(2L, BIGINT)))),

                filterTrait(
                        and(
                                eq(variable("c2", BIGINT), constant(2L, BIGINT)),
                                gt(variable("c1", BIGINT), constant(1L, BIGINT)))));
    }

    private void assertSatisfies(RowFilterTrait left, RowFilterTrait right)
    {
        assertTrue(left.satisfies(right));
    }

    @Test
    public void testFilterTraitAdd()
    {
        TraitSet traitSet = emptyTraitSet();
        // (c1 > 1) AND (c2 = 2)
        traitSet.add(
                filterTrait(
                        and(
                                gt(variable("c1", BIGINT), constant(1L, BIGINT)),
                                eq(variable("c2", BIGINT), constant(2L, BIGINT)))));
        // simulate the need when pushing a trait through filter
        // (c1 < 10) AND (c2+c3 = 2) AND c5 = 1
        traitSet.add(
                filterTrait(
                        and(
                                lt(variable("c1", BIGINT), constant(10L, BIGINT)),
                                eq(add(variable("c2", BIGINT), variable("c3", BIGINT)),
                                        constant(2L, BIGINT)),
                                eq(variable("c5", BIGINT), constant(1L, BIGINT)))));
        RowExpression predicate = traitSet.getSingle(ROW_FILTER).getPredicate();
        List<RowExpression> conjuncts = extractConjuncts(predicate);
        RowExpressionDomainTranslator.ExtractionResult result = fromPredicate(predicate);
        traitSet.listTraits();
    }

    @Test
    public void testFilterTraitTransform()
    {
        TraitSet traitSet = emptyTraitSet();
        // (c1 > 1) AND (c2 = 2)
        traitSet.add(
                filterTrait(
                        and(
                                gt(variable("c1", BIGINT), constant(1L, BIGINT)),
                                eq(variable("c2", BIGINT), constant(2L, BIGINT)))));
        // simulate the need when pushing a trait through filter
        // (c1 < 10) AND (c2+c3 = 2) AND c5 = 1
        traitSet.add(
                filterTrait(
                        and(
                                lt(variable("c1", BIGINT), constant(10L, BIGINT)),
                                eq(add(variable("c2", BIGINT), variable("c3", BIGINT)),
                                        constant(2L, BIGINT)),
                                eq(variable("c5", BIGINT), constant(1L, BIGINT)))));
        RowExpression predicate = traitSet.getSingle(ROW_FILTER).getPredicate();
        // simulate push through project
        RowExpression transformed = transform(predicate, ImmutableMap.of(variable("c2", BIGINT), variable("c5", BIGINT)), ImmutableSet.of(new Symbol("c5"), new Symbol("c1")));
        traitSet.replace(filterTrait(transformed));
        // pull up from can only work if we can reverse the assignment
        traitSet.listTraits();
    }

    private RowExpression transform(RowExpression predicate, ImmutableMap<RowExpression, RowExpression> assignments, Set<Symbol> variables)
    {
        List<RowExpression> conjuncts = extractConjuncts(canonicalizer.canonicalize(inlineExpressions(predicate, assignments)));
        return and(conjuncts.stream()
                .filter(rowExpression -> determinismEvaluator.isDeterministic(rowExpression) && variables.containsAll(extractAll(rowExpression)))
                .collect(Collectors.toList()));
    }

    private RowExpression gt(RowExpression left, RowExpression right)
    {
        return new CallExpression(resolution.comparisonFunction(ComparisonExpression.Operator.GREATER_THAN, left.getType(), right.getType()), left.getType(), ImmutableList.of(left, right));
    }

    private RowExpression lt(RowExpression left, RowExpression right)
    {
        return new CallExpression(resolution.comparisonFunction(ComparisonExpression.Operator.LESS_THAN, left.getType(), right.getType()), left.getType(), ImmutableList.of(left, right));
    }

    private RowExpression eq(RowExpression left, RowExpression right)
    {
        return new CallExpression(resolution.comparisonFunction(ComparisonExpression.Operator.EQUAL, left.getType(), right.getType()), left.getType(), ImmutableList.of(left, right));
    }

    private RowExpression add(RowExpression left, RowExpression right)
    {
        return new CallExpression(resolution.arithmeticFunction(ArithmeticBinaryExpression.Operator.ADD, left.getType(), right.getType()), left.getType(), ImmutableList.of(left, right));
    }

    private RowExpressionDomainTranslator.ExtractionResult fromPredicate(RowExpression originalPredicate)
    {
        return RowExpressionDomainTranslator.fromPredicate(metadata, TEST_SESSION, originalPredicate);
    }

    private RowFilterTrait filterTrait(RowExpression predicate)
    {
        return new RowFilterTrait(predicate, new PredicateComparator(metadata, TEST_SESSION));
    }
}