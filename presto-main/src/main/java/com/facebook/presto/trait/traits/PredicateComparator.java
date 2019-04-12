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
package com.facebook.presto.trait.traits;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.planner.optimizations.RowExpressionCanonicalizer;
import com.facebook.presto.sql.relational.DomainExtractor;

import java.util.Set;
import java.util.function.Predicate;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.relational.DomainExtractor.fromPredicate;
import static com.facebook.presto.sql.relational.LogicalRowExpressions.TRUE;
import static com.facebook.presto.sql.relational.LogicalRowExpressions.extractConjuncts;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class PredicateComparator
{
    private final Metadata metadata;
    private final Session session;
    private final RowExpressionCanonicalizer rowExpressionCanonicalizer;

    public PredicateComparator(Metadata metadata, Session session)
    {
        this.metadata = metadata;
        this.session = session;
        this.rowExpressionCanonicalizer = new RowExpressionCanonicalizer(metadata.getFunctionManager());
    }

    /**
     * Evaluates if left is a more specific predicate than right.
     * It is a best effort since we don't have extensive canonicalization, there might be false negatives.
     * For example, checkLeftSatisfiesRight(c1 + 1 > c2 , c1 > c2 - 1) will return false.
     *
     * @param left
     * @param right
     * @return
     */
    public boolean checkLeftSatisfiesRight(RowExpression left, RowExpression right)
    {
        checkArgument(left.getType() == BOOLEAN && right.getType() == BOOLEAN, "Cannot compare expression not logical");
        DomainExtractor.ExtractionResult resultLeft = fromPredicate(metadata, session, left);
        DomainExtractor.ExtractionResult resultRight = fromPredicate(metadata, session, right);

        if (!resultRight.getTupleDomain().contains(resultLeft.getTupleDomain())) {
            return false;
        }

        Set<RowExpression> leftRemaining = extractConjuncts(resultLeft.getRemainingExpression())
                .stream()
                .map(rowExpressionCanonicalizer::canonicalize)
                .filter(not(TRUE::equals))
                .collect(toImmutableSet());
        Set<RowExpression> rightRemaining = extractConjuncts(resultRight.getRemainingExpression())
                .stream()
                .map(rowExpressionCanonicalizer::canonicalize)
                .filter(not(TRUE::equals))
                .collect(toImmutableSet());
        return leftRemaining.containsAll(rightRemaining);
    }

    private static <T> Predicate<T> not(Predicate<T> p)
    {
        return p.negate();
    }
}
