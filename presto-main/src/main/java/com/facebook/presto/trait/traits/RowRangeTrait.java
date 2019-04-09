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
import com.facebook.presto.trait.Trait;

public class RowRangeTrait
        implements Trait
{
    private final RowExpression predicate;

    public RowRangeTrait(RowExpression predicate)
    {
        this.predicate = predicate;
    }

    public RowExpression getPredicate()
    {
        return predicate;
    }
}
