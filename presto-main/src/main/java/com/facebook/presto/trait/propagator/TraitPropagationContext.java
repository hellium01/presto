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
package com.facebook.presto.trait.propagator;

import com.facebook.presto.Session;
import com.facebook.presto.sql.planner.TypeProvider;

public class TraitPropagationContext
        implements TraitPropagator.Context
{
    private final Session session;
    private final TypeProvider typeProvider;

    public TraitPropagationContext(Session session, TypeProvider typeProvider)
    {
        this.session = session;
        this.typeProvider = typeProvider;
    }

    @Override
    public Session getSession()
    {
        return session;
    }

    @Override
    public TypeProvider getTypes()
    {
        return typeProvider;
    }
}
