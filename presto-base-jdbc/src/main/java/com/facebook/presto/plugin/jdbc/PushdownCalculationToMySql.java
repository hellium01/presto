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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.relation.Aggregate;
import com.facebook.presto.spi.relation.Filter;
import com.facebook.presto.spi.relation.Project;
import com.facebook.presto.spi.relation.Relation;
import com.facebook.presto.spi.relation.RelationVisitor;
import com.facebook.presto.spi.relation.TableScan;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.function.OperatorType.ADD;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.google.common.base.Preconditions.checkArgument;

public class PushdownCalculationToMySql
{

    private final static RowExpressionRewriter<List<String>> toSqlRewriter = new RowExpressionRewriter<List<String>>(
            new FunctionRule.ListBuilder<List<String>>()
                    .add(function("sum"), (currentRewriter, arguments) -> {
                        checkArgument(arguments.size() == 1);
                        Optional<List<String>> child = currentRewriter.rewrite(arguments.get(0));
                        if (!child.isPresent()) {
                            return Optional.empty();
                        }
                        return Optional.of(
                                ImmutableList.of(String.format("sum(%s)", child.get().get(0))));
                    })
                    .add(function("avg"), (currentRewriter, arguments) -> {
                        checkArgument(arguments.size() == 1);
                        Optional<List<String>> child = currentRewriter.rewrite(arguments.get(0));
                        if (!child.isPresent()) {
                            return Optional.empty();
                        }
                        return Optional.of(
                                ImmutableList.of(
                                        String.format("sum(%s)", child.get().get(0)),
                                        "count(*)"));
                    })
                    .add(operator(ADD), (currentRewriter, arguments) -> {
                        checkArgument(arguments.size() == 2);
                        Optional<List<String>> left = currentRewriter.rewrite(arguments.get(0));
                        Optional<List<String>> right = currentRewriter.rewrite(arguments.get(1));
                        if (left.isPresent() && right.isPresent()) {
                            return Optional.of(ImmutableList.of(String.format("(%s) + (%s)", left.get().get(0), right.get().get(0))));
                        }
                        return Optional.empty();
                    })
                    .build(),
            new ConstantRule.ListBuilder<List<String>>()
                    .add(BIGINT, object -> Optional.of(ImmutableList.of(String.format("%s", object))))
                    .add(INTEGER, object -> Optional.of(ImmutableList.of(String.format("%s", object))))
                    .build(),
            new ColumnRule.ListBuilder<List<String>>()
                    .add(TestColumnHandle.class, columnHandle -> Optional.of(ImmutableList.of(String.format("`%s`", ((TestColumnHandle) columnHandle).getColumnName()))))
                    .build());
    public static Optional<Relation> rewrite(Relation relation)
    {
        return relation.accept(new Visitor(), null);
    }

    public static class Visitor
            extends RelationVisitor<Optional<Relation>, Void>
    {
        @Override
        protected Optional<Relation> visitProject(Project project, Void context)
        {
            return Optional.empty();
        }

        @Override
        protected Optional<Relation> visitFilter(Filter filter, Void context)
        {
            return Optional.empty();
        }

        @Override
        protected Optional<Relation> visitAggregate(Aggregate aggregate, Void context)
        {
            return Optional.empty();
        }

        @Override
        protected Optional<Relation> visitTableScan(TableScan tableScan, Void context)
        {
            return Optional.of(tableScan);
        }
    }
}
