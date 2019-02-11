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

import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class ConstantRule<R>
        implements RewriteRule<R>
{
    private final Type type;
    private final Function<Object, Optional<R>> callback;

    public ConstantRule(Type type, Function<Object, Optional<R>> callback)
    {
        this.type = type;
        this.callback = callback;
    }

    public boolean match(RowExpression rowExpression)
    {
        if (rowExpression instanceof ConstantExpression) {
            return ((ConstantExpression) rowExpression).getType().getTypeSignature().getBase().equals(type.getTypeSignature().getBase());
        }
        return false;
    }

    public Optional<R> call(Object value)
    {
        return callback.apply(value);
    }

    public static class ListBuilder<R>
    {
        private final ImmutableList.Builder<ConstantRule<R>> builder = ImmutableList.builder();

        private static ColumnRule.ListBuilder<?> builder()
        {
            return new ColumnRule.ListBuilder<>();
        }

        public ConstantRule.ListBuilder<R> add(Type type, Function<Object, Optional<R>> callback)
        {
            builder.add(new ConstantRule<>(type, callback));
            return this;
        }

        public List<ConstantRule<R>> build()
        {
            return builder.build();
        }
    }
}
