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
package com.facebook.presto.trait.rules;

import com.facebook.presto.Session;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.optimizations.RowExpressionCanonicalizer;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.relational.DeterminismEvaluator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.trait.TraitSet;
import com.facebook.presto.trait.TraitType;
import com.facebook.presto.trait.propagator.RuleBasedTraitPropagator;
import com.facebook.presto.trait.propagator.TraitPropagator;
import com.facebook.presto.trait.propagator.TraitProvider;
import com.facebook.presto.trait.traits.RowFilterTrait;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.SymbolsExtractor.extractAll;
import static com.facebook.presto.sql.relational.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.sql.relational.RowExpressionUtils.inlineExpressions;
import static com.facebook.presto.sql.relational.SqlToRowExpressionTranslator.translate;
import static com.facebook.presto.trait.BasicTraitSet.emptyTraitSet;
import static com.facebook.presto.trait.traits.RowFilterTraitType.ROW_FILTER;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class FilterPushdownRule
        implements RuleBasedTraitPropagator.PushDownRule<RowFilterTrait, PlanNode>
{
    private final RowExpressionCanonicalizer canonicalizer;
    private final Metadata metadata;
    private final SqlParser parser;
    private final DeterminismEvaluator determinismEvaluator;

    public FilterPushdownRule(Metadata metadata)
    {
        requireNonNull(metadata, "metadata is null");
        this.canonicalizer = new RowExpressionCanonicalizer(metadata.getFunctionManager());
        this.parser = new SqlParser();
        this.determinismEvaluator = new DeterminismEvaluator(metadata.getFunctionManager());
        this.metadata = metadata;
    }

    @Override
    public TraitType<RowFilterTrait> getTraitType()
    {
        return ROW_FILTER;
    }

    @Override
    public Class<PlanNode> getPlanNodeType()
    {
        return PlanNode.class;
    }

    @Override
    public void pushDown(PlanNode planNode, TraitProvider traitProvider, TraitPropagator.Context context)
    {
        planNode.accept(new PushDownVisitor(
                context.getSession(),
                context.getTypes(),
                metadata,
                parser,
                traitProvider,
                canonicalizer,
                determinismEvaluator), traitProvider);
    }

    private static class PushDownVisitor
            extends PlanVisitor<TraitSet, TraitProvider>
    {
        private final TypeProvider types;
        private final TraitProvider provider;
        private final TransformationUtils transformer;

        public PushDownVisitor(
                Session session,
                TypeProvider types,
                Metadata metadata,
                SqlParser sqlParser,
                TraitProvider provider,
                RowExpressionCanonicalizer canonicalizer,
                DeterminismEvaluator determinismEvaluator)
        {
            this.transformer = new TransformationUtils(session, metadata, sqlParser, canonicalizer, determinismEvaluator);
            this.types = types;
            this.provider = provider;
        }

        @Override
        protected TraitSet visitPlan(PlanNode node, TraitProvider traits)
        {
            return emptyTraitSet();
        }
    }

    private static class TransformationUtils
    {

        private final Session session;
        private final Metadata metadata;
        private final SqlParser sqlParser;
        private final RowExpressionCanonicalizer canonicalizer;
        private final DeterminismEvaluator determinismEvaluator;

        public TransformationUtils(Session session, Metadata metadata, SqlParser sqlParser, RowExpressionCanonicalizer canonicalizer, DeterminismEvaluator determinismEvaluator)
        {
            this.session = session;
            this.metadata = metadata;
            this.sqlParser = sqlParser;
            this.canonicalizer = canonicalizer;
            this.determinismEvaluator = determinismEvaluator;
        }

        private RowExpression toRowExpression(Expression expression, TypeProvider types)
        {
            // replace qualified names with input references since row expressions do not support these

            // determine the type of every expression
            Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(
                    session,
                    metadata,
                    sqlParser,
                    types,
                    expression,
                    emptyList(), /* parameters have already been replaced */
                    WarningCollector.NOOP);

            // convert to row expression
            return translate(expression, expressionTypes, ImmutableMap.of(), metadata.getFunctionManager(), metadata.getTypeManager(), session, false);
        }

        private RowFilterTrait extractConjunctsCoversOutput(RowFilterTrait input, List<Symbol> outputSymbols)
        {
            return input.newPredicate(transform(input.getPredicate(), ImmutableMap.of(), ImmutableSet.copyOf(outputSymbols)));
        }

        private RowFilterTrait extractConjunctsCoversOutput(RowFilterTrait input, Map<RowExpression, RowExpression> assignements, List<Symbol> outputSymbols)
        {
            return input.newPredicate(transform(input.getPredicate(), ImmutableMap.of(), ImmutableSet.copyOf(outputSymbols)));
        }

        private RowExpression transform(RowExpression predicate, Map<RowExpression, RowExpression> assignments, Set<Symbol> variables)
        {
            List<RowExpression> conjuncts = extractConjuncts(canonicalizer.canonicalize(inlineExpressions(predicate, assignments)));
            return new SpecialFormExpression(
                    AND,
                    BOOLEAN,
                    conjuncts.stream()
                            .filter(rowExpression -> determinismEvaluator.isDeterministic(rowExpression) && variables.containsAll(extractAll(rowExpression)))
                            .collect(toImmutableList()));
        }
    }

    public static RowExpression and(RowExpression... conjuncts)
    {
        return new SpecialFormExpression(AND, BOOLEAN, conjuncts);
    }
}
