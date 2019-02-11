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
import com.facebook.presto.spi.connector.ConnectorOptimizationRule;
import com.facebook.presto.spi.connector.ConnectorRuleProvider;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import java.util.Set;

public class JdbcConnetorRuleProvider
        implements ConnectorRuleProvider
{
    private Set<ConnectorOptimizationRule> rules;

    @Inject
    public JdbcConnetorRuleProvider()
    {
        this.rules = allRules();
    }

    private Set<ConnectorOptimizationRule> allRules()
    {
        return new ImmutableSet.Builder()
                .add(new PredicatePushdown())
                .add(new ProjectPushdown())
                .add(new AggregationPushdown())
                .build();
    }

    @Override
    public Set<ConnectorOptimizationRule> getRules()
    {
        return rules;
    }
}
