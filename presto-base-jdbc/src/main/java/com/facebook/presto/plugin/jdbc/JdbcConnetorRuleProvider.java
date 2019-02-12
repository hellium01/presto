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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.plugin.jdbc.rewriter.AggregationPushdown;
import com.facebook.presto.plugin.jdbc.rewriter.PredicatePushdown;
import com.facebook.presto.plugin.jdbc.rewriter.ProjectPushdown;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorOptimizationRule;
import com.facebook.presto.spi.connector.ConnectorRuleProvider;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class JdbcConnetorRuleProvider
        implements ConnectorRuleProvider
{
    private final Set<ConnectorOptimizationRule> rules;

    public JdbcConnetorRuleProvider(String connectorId, ConnectorContext context)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(context, "context is null");
        this.rules = allRules(connectorId, context);
    }

    private Set<ConnectorOptimizationRule> allRules(String connectorId, ConnectorContext context)
    {
        return new ImmutableSet.Builder()
                .add(new PredicatePushdown())
                .add(new ProjectPushdown(connectorId))
                .add(new AggregationPushdown(connectorId, context.getTypeManager()))
                .build();
    }

    @Override
    public Set<ConnectorOptimizationRule> getRules()
    {
        return rules;
    }
}
