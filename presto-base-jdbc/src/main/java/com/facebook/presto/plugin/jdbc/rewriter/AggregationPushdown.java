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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorOptimizationRule;
import com.facebook.presto.spi.relation.Aggregate;
import com.facebook.presto.spi.relation.Relation;

import java.util.Optional;

public class AggregationPushdown
        implements ConnectorOptimizationRule
{
    @Override
    public boolean enabled(ConnectorSession session)
    {
        return true;
    }

    @Override
    public boolean match(ConnectorSession session, Relation relation)
    {
        return relation instanceof Aggregate;
    }

    @Override
    public Optional<Relation> optimize(Relation relation)
    {
        return Optional.empty();
    }
}
