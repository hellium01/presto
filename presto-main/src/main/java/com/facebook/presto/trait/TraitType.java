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
package com.facebook.presto.trait;

import java.util.List;

public abstract class TraitType<T extends Trait>
{
    private boolean mergable;
    private boolean allowMulti;

    public TraitType(boolean mergable, boolean allowMulti)
    {
        this.mergable = mergable;
        this.allowMulti = allowMulti;
    }

    public boolean isMergable()
    {
        return mergable;
    }

    public boolean isAllowMulti()
    {
        return allowMulti;
    }

    /**
     * If trait type is mergable, it needs to implement this method to combine multiple traits into single value.
     * @param list of traits of same type.
     * @return merged trait value.
     */
    public T merge(List<T> traits)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * If trait type allows multiple values, it needs to implement this method to deduplicate.
     * @param list of traits of same type.
     * @return deduplicated list of traits in same order
     */
    public List<T> deduplicate(List<T> traits)
    {
        throw new UnsupportedOperationException();
    }
}
