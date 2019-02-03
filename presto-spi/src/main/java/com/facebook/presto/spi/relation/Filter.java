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

import java.util.List;

public class Filter
        extends UnaryNode
{
    private final RowExpression predicate;
    private final Relation source;

    public Filter(RowExpression predicate, Relation source)
    {
        this.predicate = predicate;
        this.source = source;
    }

    public RowExpression getPredicate()
    {
        return predicate;
    }

    @Override
    public List<RowExpression> getOutput()
    {
        return source.getOutput();
    }

    @Override
    public <R, C> R accept(RelationVisitor<R, C> visitor, C context)
    {
        return visitor.visitFilter(this, context);
    }

    @Override
    public Relation getSource()
    {
        return source;
    }
}
