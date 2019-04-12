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

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.scalar.FunctionAssertions.createExpression;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.util.stream.Collectors.toMap;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPredicateComparator
{
    private final TestingRowExpressionTranslator translator = new TestingRowExpressionTranslator();
    private final TypeProvider types = createTypeProvider(new ImmutableMap.Builder<String, Type>()
            .put("c1", BIGINT)
            .put("c2", DOUBLE)
            .put("c3", VARCHAR)
            .build());
    private final Metadata metadata = MetadataManager.createTestMetadataManager();

    @Test
    public void testCompare()
    {
        assertSatisfies("c1 > c2 AND c2 > 1.0", "true");
        assertNotSatisfies("true", "c1 > c2 AND c2 > 1.0");
        assertNotSatisfies("c1 > c2 AND c2 > 1.0", "false");
        assertNotSatisfies("false", "c1 > c2 AND c2 > 1.0");

        assertSatisfies("c1 > 1 AND c2 = 1.0", "c1 > 0 AND c2 = 1.0");
        assertNotSatisfies("c1 > 0 AND c2 = 1.0", "c1 > 1 AND c2 = 1.0");

        assertSatisfies("NOT c1 IN (1, 2, 3) AND c2 = 1.0", "NOT c1 IN (1, 2) AND c2 = 1.0");
        assertSatisfies("c1 IN (1, 2) AND c2 = 1.0", "c1 IN (1, 2, 3) AND c2 = 1.0");

        assertSatisfies("c1 > 1 +2 AND c2 = 1.0", "c1 > 0 AND c2 = 1.0");

        assertNotSatisfies("c1 > random() AND c2 = 1.0", "c1 > 0 AND c2 = 1.0");

        assertSatisfies("c1 > c2 AND c2 > 1.0", "c1 > c2 AND c2 > 0.0");
        assertSatisfies("c1 > c2 AND c2 > 1.0", "c2 < c1 AND 0.0 < c2");

        // should left over only c1 > c2
        assertSatisfies("c1 > c2 AND c2 > 1.0 AND c3 IN ('c1', 'c2', 'c3')", "c1> c2 AND c2 > 0.0");
        // something cannot be evaluated into constant
        assertSatisfies("c1 > c2 AND c2 > 1.0 AND c3 IN ('c1', 'c2', 'c3', regexp_extract('c12', 'c1'))", "c1> c2 AND c2 > 0.0");
        assertSatisfies("c1 > c2 AND c2 > 1.0 AND c3 NOT IN ('c1', 'c2', 'c3', regexp_extract('c12', 'c1'))", "c1> c2 AND c2 > 0.0");
        // constant part of in list will be extracted into domain thus comparable
        assertSatisfies("c1 > c2 AND c2 > 1.0 AND c3 NOT IN ('c1', 'c2', 'c3', regexp_extract('c12', 'c1'))", "c1> c2 AND c2 > 0.0 AND c3 NOT IN('c1', 'c2')");

        // We are not smart enough to eliminate the +1 from both side
        assertNotSatisfies("c1 > c2 AND c2 > 1.0", "c1 + 1 > c2 +1 AND c2 > 0.0");

        assertSatisfies("c1 +1 > 1 AND c2 = 1.0", "c1 +1 > 0 AND c2 = 1.0");
        // We are not smart enough to figure out arithmetic
        assertNotSatisfies("c1  > 1 AND c2 = 1.0", "c1 + 1 > 0 AND c2 = 1.0");
    }

    private void assertSatisfies(String predicateMoreSpecific, String predicateLessSpecific)
    {
        RowExpression rowExpressionMoreSpecific = translator.translate(expression(predicateMoreSpecific), types);
        RowExpression rowExpressionLessSpecific = translator.translate(expression(predicateLessSpecific), types);
        PredicateComparator comparator = new PredicateComparator(metadata, TEST_SESSION);
        assertTrue(comparator.checkLeftSatisfiesRight(rowExpressionMoreSpecific, rowExpressionLessSpecific));
    }

    private void assertNotSatisfies(String predicateMoreSpecific, String predicateLessSpecific)
    {
        RowExpression rowExpressionMoreSpecific = translator.translate(expression(predicateMoreSpecific), types);
        RowExpression rowExpressionLessSpecific = translator.translate(expression(predicateLessSpecific), types);
        PredicateComparator comparator = new PredicateComparator(metadata, TEST_SESSION);
        assertFalse(comparator.checkLeftSatisfiesRight(rowExpressionMoreSpecific, rowExpressionLessSpecific));
    }

    private Expression expression(String sql)
    {
        return createExpression(sql, metadata, types);
    }

    private static TypeProvider createTypeProvider(Map<String, Type> types)
    {
        return TypeProvider.viewOf(types.entrySet().stream().collect(toMap(entry -> new Symbol(entry.getKey()), entry -> entry.getValue())));
    }
}
