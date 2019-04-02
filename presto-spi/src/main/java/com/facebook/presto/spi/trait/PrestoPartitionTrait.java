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
package com.facebook.presto.spi.trait;

import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public class PrestoPartitionTrait<T>
{
    private final Optional<Boolean> distributed;
    // Description of the partitioning of the data across nodes
    private final Optional<ConnectorPartitioningHandle> handle;
    private final Optional<Set<T>> partionSymbol; // if missing => partitioned with some unknown scheme
    // Description of the partitioning of the data across streams (splits)
    private final Optional<Set<T>> streamingParitionSymbol;
    private final Optional<Boolean> nullsAndAnyReplicated;

    // NOTE: Partitioning on zero columns (or effectively zero columns if the columns are constant) indicates that all
    // the rows will be partitioned into a single node or stream. However, this can still be a partitioned plan in that the plan
    // will be executed on multiple servers, but only one server will get all the data.

    // Description of whether rows with nulls in partitioning columns or some arbitrary rows have been replicated to all *nodes*

    public static class Partitioning
    {
        private final ConnectorPartitioningHandle connectorHandle;
        private final List<ArgumentBinding> arguments;
    }

    private static class ArgumentBinding {}
}
