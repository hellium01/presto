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
package com.facebook.presto.spi.relation.column;

import com.facebook.presto.spi.type.Type;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class Constant<T>
        extends ColumnExpression<T>
{
    private final Object value;
    private final Type type;

    public Constant(Object value, Type type)
    {
        requireNonNull(type, "type is null");

        this.value = value;
        this.type = type;
    }

    public Object getValue()
    {
        return value;
    }

    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return String.valueOf(value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, type);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Constant other = (Constant) obj;
        return Objects.equals(this.value, other.value) && Objects.equals(this.type, other.type);
    }

    @Override
    public <R, C> R accept(ColumnExpressionVisitor<T, R, C> visitor, C context)
    {
        return visitor.visitConstant(this, context);
    }
}