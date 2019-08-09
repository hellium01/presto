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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.LogicalRowExpressions;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionRewriter;
import com.facebook.presto.spi.relation.RowExpressionTreeRewriter;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionOptimizer;
import com.google.common.annotations.VisibleForTesting;

import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.SERIALIZABLE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.OR;
import static java.util.Objects.requireNonNull;

public class SimplifyRowExpressions
        extends RowExpressionRewriteRuleSet
{
    public SimplifyRowExpressions(Metadata metadata)
    {
        super(new Rewriter(metadata));
    }

    private static class Rewriter
            implements PlanRowExpressionRewriter
    {
        private final Metadata metadata;
        private final RowExpressionOptimizer optimizer;

        public Rewriter(Metadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.optimizer = new RowExpressionOptimizer(metadata);
        }

        @Override
        public RowExpression rewrite(RowExpression expression, Rule.Context context)
        {
            return rewrite(expression, context.getSession().toConnectorSession());
        }

        private RowExpression rewrite(RowExpression expression, ConnectorSession session)
        {
            RowExpression optimized = optimizer.optimize(expression, SERIALIZABLE, session);
            if (optimized instanceof ConstantExpression || !BooleanType.BOOLEAN.equals(optimized.getType())) {
                return optimized;
            }
            return RowExpressionTreeRewriter.rewriteWith(new LogicalExpressionRewriter(metadata), optimized);
        }
    }

    @VisibleForTesting
    public static RowExpression rewrite(RowExpression expression, Metadata metadata, ConnectorSession session)
    {
        return new Rewriter(metadata).rewrite(expression, session);
    }

    private static class LogicalExpressionRewriter
            extends RowExpressionRewriter<Void>
    {
        private final FunctionResolution functionResolution;
        private final LogicalRowExpressions logicalRowExpressions;

        public LogicalExpressionRewriter(Metadata metadata)
        {
            this.functionResolution = new FunctionResolution(metadata.getFunctionManager());
            this.logicalRowExpressions = new LogicalRowExpressions(new RowExpressionDeterminismEvaluator(metadata.getFunctionManager()), new FunctionResolution(metadata.getFunctionManager()));
        }

        @Override
        public RowExpression rewriteCall(CallExpression node, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
        {
            if (BooleanType.BOOLEAN.equals(node.getType()) && functionResolution.isNotFunction(node.getFunctionHandle())) {
                return rewriteBooleanExpression(node);
            }
            return null;
        }

        @Override
        public RowExpression rewriteSpecialForm(SpecialFormExpression node, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
        {
            if (BooleanType.BOOLEAN.equals(node.getType()) && isConjunctiveDisjunctive(node.getForm())) {
                return rewriteBooleanExpression(node);
            }
            return null;
        }

        private boolean isConjunctiveDisjunctive(Form form)
        {
            return form == AND || form == OR;
        }

        private RowExpression rewriteBooleanExpression(RowExpression expression)
        {
            return logicalRowExpressions.convertToConjunctiveNormalForm(expression);
        }
    }
}
