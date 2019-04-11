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

import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.trait.Trait;
import com.facebook.presto.trait.TraitType;

import java.util.Objects;

import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.facebook.presto.trait.traits.ConstantTraitType.CONSTANT_TRAIT_TYPE;
import static java.util.Objects.requireNonNull;

public class ConstantTrait
        implements Trait
{
    private final VariableReferenceExpression name;
    private final Object value;

    public ConstantTrait(VariableReferenceExpression name, Object value)
    {
        this.name = requireNonNull(name, "name is null");
        this.value = value;
    }

    public VariableReferenceExpression getVariable()
    {
        return name;
    }

    public ConstantExpression getValue()
    {
        if (value != null) {
            return constant(value, name.getType());
        }
        return constantNull(name.getType());
    }

    @Override
    public TraitType<?> getTraitType()
    {
        return CONSTANT_TRAIT_TYPE;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConstantTrait)) {
            return false;
        }
        ConstantTrait that = (ConstantTrait) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, value);
    }
}
