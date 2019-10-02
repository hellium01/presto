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
package com.facebook.presto.plugin.jdbc.optimization;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.LogicalRowExpressions;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.relation.translator.RowExpressionTranslator;
import com.facebook.presto.spi.relation.translator.RowExpressionTreeTranslator;
import com.facebook.presto.spi.relation.translator.TranslatedExpression;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.plugin.jdbc.optimization.PredicateTranslator.Result.fullyPushable;
import static com.facebook.presto.plugin.jdbc.optimization.PredicateTranslator.Result.unpushable;
import static com.facebook.presto.spi.relation.LogicalRowExpressions.and;
import static com.facebook.presto.spi.relation.LogicalRowExpressions.or;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class PredicateTranslator
        extends RowExpressionTranslator<PredicateTranslator.Result, Map<VariableReferenceExpression, ColumnHandle>>
{
    private RowExpressionToSqlTranslator translator;
    private LogicalRowExpressions logicalRowExpressions;

    @Override
    public Optional<TranslatedExpression<Result>> translateExpression(RowExpression expression, Map<VariableReferenceExpression, ColumnHandle> context, RowExpressionTreeTranslator<Result, Map<VariableReferenceExpression, ColumnHandle>> rowExpressionTreeTranslator)
    {
        TranslatedExpression<JdbcSql> result = RowExpressionTreeTranslator.translateWith(expression, translator, context);
        if (result.getTranslated().isPresent()) {
            return Optional.of(new TranslatedExpression<>(Optional.of(fullyPushable(expression, result.getTranslated().get())), expression, ImmutableList.of()));
        }
        return Optional.of(new TranslatedExpression<>(Optional.of(unpushable(expression)), expression, ImmutableList.of()));
    }

    @Override
    public Optional<TranslatedExpression<Result>> translateSpecialForm(SpecialFormExpression specialForm, Map<VariableReferenceExpression, ColumnHandle> context, RowExpressionTreeTranslator<Result, Map<VariableReferenceExpression, ColumnHandle>> rowExpressionTreeTranslator)
    {
        List<Result> translatedClauses = specialForm.getArguments().stream()
                .map(argument -> rowExpressionTreeTranslator.translate(argument, context))
                .map(TranslatedExpression::getTranslated)
                .map(Optional::get)
                .collect(toImmutableList());
        List<String> pushedClauses = translatedClauses.stream()
                .map(Result::getPushedSql)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(JdbcSql::getSql)
                .collect(toImmutableList());
        List<ConstantExpression> bindVariables = translatedClauses.stream()
                .map(Result::getPushedSql)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(JdbcSql::getBindValues)
                .flatMap(List::stream)
                .collect(toImmutableList());
        List<RowExpression> leftConjuncts = translatedClauses.stream()
                .map(Result::getLeftPredicate)
                .flatMap(List::stream)
                .collect(toImmutableList());
        switch (specialForm.getForm()) {
            case AND: {
                /**
                 * All fullyPushable -> fully pushable
                 */
                if (pushedClauses.isEmpty()) {
                    return Optional.of(new TranslatedExpression<>(Optional.of(unpushable(specialForm)), specialForm, ImmutableList.of()));
                }
                JdbcSql newJdbcSql = new JdbcSql(Joiner.on(" AND ").join(pushedClauses), bindVariables);
                return Optional.of(new TranslatedExpression<>(Optional.of(new Result(specialForm, Optional.of(newJdbcSql), leftConjuncts)), specialForm, ImmutableList.of()));
            }
            case OR: {
                /**
                 * ( A [pushable] && B [unpushable]) || C [pushable] -> (A || C) [pushable] && (B || C) [unpushable]
                 */
                // If any clause is unpushable, we cannot push anything.
                if (pushedClauses.size() != translatedClauses.size()) {
                    return Optional.of(new TranslatedExpression<>(Optional.of(unpushable(specialForm)), specialForm, ImmutableList.of()));
                }
                // We should only have one clause is not fully pushable
                List<Result> clausesFullyPushable = translatedClauses.stream().filter(Result::notFullyPushable).collect(toImmutableList());
                // More than one clause is not fully pushable is not pushable.
                if (clausesFullyPushable.size() < translatedClauses.size() - 1) {
                    return Optional.of(new TranslatedExpression<>(Optional.of(unpushable(specialForm)), specialForm, ImmutableList.of()));
                }
                JdbcSql newJdbcSql = new JdbcSql(Joiner.on(" OR ").join(pushedClauses), bindVariables);
                if (clausesFullyPushable.size() == translatedClauses.size()) {
                    // fully pushable
                    return Optional.of(new TranslatedExpression<>(Optional.of(new Result(specialForm, Optional.of(newJdbcSql), ImmutableList.of())), specialForm, ImmutableList.of()));
                }
                Result partiallyPushable = clausesFullyPushable.get(0);
                List<RowExpression> newConjuncts = translatedClauses.stream()
                        .filter(Result::isFullyPushable)
                        .map(Result::getOriginalExpression)
                        .map(expression -> or(expression, and(partiallyPushable.getLeftPredicate())))
                        .collect(toImmutableList());
                return Optional.of(new TranslatedExpression<>(Optional.of(new Result(specialForm, Optional.of(newJdbcSql), newConjuncts)), specialForm, ImmutableList.of()));
            }
        }
        return translateExpression(specialForm, context, rowExpressionTreeTranslator);
    }

    public static class Result
    {
        private final RowExpression originalExpression;
        private final Optional<JdbcSql> pushedSql;
        private final List<RowExpression> leftConjuncts;

        private Result(RowExpression originalExpression, Optional<JdbcSql> pushedSql, List<RowExpression> leftConjuncts)
        {
            this.originalExpression = originalExpression;
            this.pushedSql = pushedSql;
            this.leftConjuncts = leftConjuncts;
        }

        public Optional<JdbcSql> getPushedSql()
        {
            return pushedSql;
        }

        public RowExpression getOriginalExpression()
        {
            return originalExpression;
        }

        public List<RowExpression> getLeftPredicate()
        {
            return leftConjuncts;
        }

        public boolean isFullyPushable()
        {
            return pushedSql.isPresent() && leftConjuncts.isEmpty();
        }

        public boolean notFullyPushable()
        {
            return !pushedSql.isPresent() || !leftConjuncts.isEmpty();
        }

        public static Result fullyPushable(RowExpression expression, JdbcSql jdbcSql)
        {
            return new Result(expression, Optional.of(jdbcSql), ImmutableList.of());
        }

        public static Result unpushable(RowExpression expression)
        {
            return new Result(expression, Optional.empty(), ImmutableList.of(expression));
        }
    }
}
