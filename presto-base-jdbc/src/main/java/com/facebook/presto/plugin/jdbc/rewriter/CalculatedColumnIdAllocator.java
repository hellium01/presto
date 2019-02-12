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

import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class CalculatedColumnIdAllocator
{
    private int nextId;
    private static String PREFIX = "calculated_";
    private final Set<String> existingNames;

    public CalculatedColumnIdAllocator(Set<String> existingNames)
    {
        this.existingNames = new HashSet<>(requireNonNull(existingNames, "existingNames is empty"));
    }

    public CalculatedColumnIdAllocator()
    {
        this(ImmutableSet.of());
    }

    public String getNextId()
    {
        String id = PREFIX + Integer.toString(nextId++);
        if (existingNames.contains(id)) {
            return getNextId();
        }
        return id;
    }
}
