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
package com.facebook.presto.raptorx.util;

import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import java.util.List;

import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Locale.ENGLISH;

public final class PropertyUtil
{
    private PropertyUtil() {}

    public static PropertyMetadata<String> lowerCaseStringProperty(String name, String description)
    {
        return new PropertyMetadata<>(
                name,
                description,
                createUnboundedVarcharType(),
                String.class,
                null,
                false,
                value -> ((String) value).toLowerCase(ENGLISH),
                value -> value);
    }

    public static PropertyMetadata<?> lowerCaseStringListProperty(TypeManager typeManager, String name, String description)
    {
        return new PropertyMetadata<>(
                name,
                description,
                typeManager.getParameterizedType(ARRAY, ImmutableList.of(TypeSignatureParameter.of(createUnboundedVarcharType().getTypeSignature()))),
                List.class,
                ImmutableList.of(),
                false,
                value -> stringList(value).stream()
                        .map(s -> s.toLowerCase(ENGLISH))
                        .collect(toImmutableList()),
                value -> value);
    }

    public static PropertyMetadata<DataSize> dataSizeProperty(String name, String description, DataSize defaultValue, boolean hidden)
    {
        return new PropertyMetadata<>(
                name,
                description,
                createUnboundedVarcharType(),
                DataSize.class,
                defaultValue,
                hidden,
                value -> DataSize.valueOf((String) value),
                DataSize::toString);
    }

    @SuppressWarnings("unchecked")
    public static List<String> stringList(Object value)
    {
        return (value == null) ? ImmutableList.of() : ((List<String>) value);
    }
}
