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

public enum Order
{
    ASC_NULLS_FIRST(true, true, true, false),
    ASC_NULLS_LAST(true, false, true, false),
    DESC_NULLS_FIRST(false, true, true, false),
    DESC_NULLS_LAST(false, false, true, false),
    LIKELY_ASC_NULLS_FIRST(true, true, false, false),
    LIKELY_ASC_NULLS_LAST(true, false, false, false),
    LIKELY_DESC_NULLS_FIRST(false, true, false, false),
    LIKELY_DESC_NULLS_LAST(false, false, false, false),
    CONSTANT(false, false, true, true);

    private final boolean ascending;
    private final boolean nullsFirst;
    private final boolean strict;
    private final boolean fixed;

    Order(boolean ascending, boolean nullsFirst, boolean strict, boolean fixed)
    {
        this.ascending = ascending;
        this.nullsFirst = nullsFirst;
        this.strict = strict;
        this.fixed = fixed;
    }

    public boolean isAscending()
    {
        return ascending;
    }

    public boolean isNullsFirst()
    {
        return nullsFirst;
    }

    public boolean isStrict()
    {
        return strict;
    }

    public boolean isFixed()
    {
        return fixed;
    }
}
