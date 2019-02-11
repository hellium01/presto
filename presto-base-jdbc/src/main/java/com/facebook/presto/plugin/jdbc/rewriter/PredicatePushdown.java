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
package com.facebook.presto.plugin.jdbc.rewriter;

import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTableLayoutHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorOptimizationRule;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.Filter;
import com.facebook.presto.spi.relation.Relation;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.TableScan;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.plugin.jdbc.rewriter.ExpressionoSqlTranslator.RewriteInput.rewriteInput;
import static com.facebook.presto.plugin.jdbc.rewriter.ExpressionoSqlTranslator.translateToSQL;
import static com.facebook.presto.plugin.jdbc.rewriter.FunctionPattern.function;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.builder;

public class PredicatePushdown
        implements ConnectorOptimizationRule
{
    private static Signature AND_SIGNATURE = new Signature("and", FunctionKind.SCALAR, BOOLEAN.getTypeSignature(), ImmutableList.of(BOOLEAN.getTypeSignature(), BOOLEAN.getTypeSignature()));

    @Override
    public boolean enabled(ConnectorSession session)
    {
        return true;
    }

    @Override
    public boolean match(ConnectorSession session, Relation relation)
    {
        return relation instanceof Filter && ((Filter) relation).getSource() instanceof TableScan;
    }

    @Override
    public Optional<Relation> optimize(Relation relation)
    {
        checkArgument(relation instanceof Filter, "relation is not Filter");
        Filter filter = (Filter) relation;
        TableScan tableScan = (TableScan) filter.getSource();
        RowExpression predicate = rewriteInput(filter.getPredicate(), filter.getSource().getOutput());
        Optional<Result> result = getPredicateRewriter().rewrite(predicate);
        if (result.isPresent() && !result.get().getRewrittenSQL().isEmpty()) {
            TableScan newTableScan = tableScan.withTableLayout(
                    new JdbcTableLayoutHandle(
                            (JdbcTableHandle) tableScan.getTableHandle(),
                            TupleDomain.all(),
                            Optional.of(Joiner.on(" AND ").join(result.get().getRewrittenSQL()))));
            if (result.get().getLeftOver() != null) {
                return Optional.of(new Filter(result.get().getLeftOver(), newTableScan));
            }
            return Optional.of(newTableScan);
        }
        return Optional.empty();
    }

    private RowExpressionRewriter<Result> getPredicateRewriter()
    {
        return new RowExpressionRewriter<Result>(
                new FunctionRule.ListBuilder<Result>()
                        .add(function("AND"), (rewriter, callExpression) -> {
                            List<RowExpression> arguments = callExpression.getArguments();
                            checkArgument(arguments.size() == 2, "AND must be binary operation");
                            RowExpression left = arguments.get(0);
                            RowExpression right = arguments.get(1);
                            Optional<String> rewritten = translateToSQL(callExpression);
                            ImmutableList.Builder<String> sql = builder();
                            RowExpression leftOver = null;
                            if (rewritten.isPresent()) {
                                sql.add(rewritten.get());
                            }
                            else {
                                Optional<Result> leftResult = rewriter.rewrite(left);
                                if (leftResult.isPresent()) {
                                    sql.addAll(leftResult.get().getRewrittenSQL());
                                    leftOver = merge(leftOver, leftResult.get().getLeftOver());
                                }
                                else {
                                    leftOver = merge(leftOver, left);
                                }
                                Optional<Result> rightResult = rewriter.rewrite(right);
                                if (rightResult.isPresent()) {
                                    sql.addAll(rightResult.get().getRewrittenSQL());
                                    leftOver = merge(leftOver, rightResult.get().getLeftOver());
                                }
                                else {
                                    leftOver = merge(leftOver, right);
                                }
                            }
                            return new Result(sql.build(), leftOver);
                        })
                        .build(),
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty());
    }

    private RowExpression merge(RowExpression left, RowExpression right)
    {
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        checkArgument(left.getType().equals(BOOLEAN) && right.getType().equals(BOOLEAN), "left/right must both be boolean type");
        return new CallExpression(AND_SIGNATURE, BOOLEAN, ImmutableList.of(left, right));
    }

    private static class Result
    {
        private final List<String> rewrittenSQL;
        private final RowExpression leftOver;

        public Result(List<String> rewrittenSQL, RowExpression leftOver)
        {
            this.rewrittenSQL = rewrittenSQL;
            this.leftOver = leftOver;
        }

        public List<String> getRewrittenSQL()
        {
            return rewrittenSQL;
        }

        public RowExpression getLeftOver()
        {
            return leftOver;
        }
    }
}
