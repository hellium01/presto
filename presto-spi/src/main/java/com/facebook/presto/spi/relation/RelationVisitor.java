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
package com.facebook.presto.spi.relation;

public abstract class RelationVisitor<R, C>
{
    protected abstract R visitProject(Project project, C context);

    protected abstract R visitFilter(Filter filter, C context);

    protected abstract R visitAggregate(Aggregate aggregate, C context);

    protected abstract R visitTableScan(TableScan tableScan, C context);
}
