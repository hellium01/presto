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
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.relation.Aggregate;
import com.facebook.presto.spi.relation.Filter;
import com.facebook.presto.spi.relation.Project;
import com.facebook.presto.spi.relation.RelationVisitor;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.TableScan;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.relational.rewriter.FunctionRule;
import com.facebook.presto.sql.relational.rewriter.RowExpressionRewriter;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.facebook.presto.spi.function.OperatorType.ADD;
import static com.facebook.presto.sql.relational.rewriter.FunctionPattern.operator;

public class PlanGenerator
{
    private final ConnectorId connectorId;
    private PlanNodeIdAllocator idAllocator;


    private class PlanCreator
            extends RelationVisitor<Optional<PlanNode>, Void>
    {
        @Override
        protected Optional<PlanNode> visitProject(Project project, Void context)
        {
            return Optional.empty();
        }

        @Override
        protected Optional<PlanNode> visitFilter(Filter filter, Void context)
        {
            return Optional.empty();
        }

        @Override
        protected Optional<PlanNode> visitAggregate(Aggregate aggregate, Void context)
        {
            return Optional.empty();
        }

        @Override
        protected Optional<PlanNode> visitTableScan(TableScan tableScan, Void context)
        {
            return Optional.of(new TableScanNode(
                    idAllocator.getNextId(),
                    new TableHandle(connectorId, tableScan.getTableHandle()),

            ));
        }

        private Optional<Expression> rewriteRowExpression(RowExpression rowExpression)
        {
            RowExpressionRewriter<Expression> rewriter = new RowExpressionRewriter<Expression>(
             ImmutableList.of(
                     new FunctionRule(operator(ADD), (myRewriter, arguments) -> {

                        return new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.ADD,  );
                     }),
                     ImmutableList.of(),
                     ImmutableList.of(),
                     Optional.empty()
             );
             return rewriter.rewrite(rowExpression);
        }
    }
}
