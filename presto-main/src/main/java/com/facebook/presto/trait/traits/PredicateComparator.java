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

import com.facebook.presto.spi.relation.RowExpression;

public interface PredicateComparator
{
    /**
     * Evaluates if left is a more specific predicate than right.
     * It is a best effort since we don't have extensive canonicalization, there might be false negatives.
     * For example, checkLeftSatisfiesRight(c1 + 1 > c2 , c1 > c2 - 1) will return false.
     *
     * @param left predicate represented by row expression.
     * @param right predicate represented by row expression.
     * @return if left is more specific than right
     */
    boolean checkLeftSatisfiesRight(RowExpression left, RowExpression right);
}
