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

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.trait.Trait;
import com.facebook.presto.trait.TraitType;

import static com.facebook.presto.trait.traits.DataSourceTraitType.DATA_SOURCE_TRAIT;

public class DataSourceTrait
        implements Trait
{
    private final ConnectorId connectorId;

    public DataSourceTrait(ConnectorId connectorId)
    {
        this.connectorId = connectorId;
    }

    @Override
    public boolean satisfies(Trait other)
    {
        if (other instanceof DataSourceTrait) {
            return satisfies((DataSourceTrait) other);
        }
        return false;
    }

    public boolean satisfies(DataSourceTrait other)
    {
        return other.connectorId == other.connectorId;
    }

    @Override
    public boolean isEnforced()
    {
        return true;
    }

    @Override
    public TraitType<?> getTraitType()
    {
        return DATA_SOURCE_TRAIT;
    }
}
