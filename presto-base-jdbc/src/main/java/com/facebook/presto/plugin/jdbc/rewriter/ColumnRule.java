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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.relation.ColumnReferenceExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ColumnRule<R>
        implements RewriteRule<R>
{
    private final Class<?> columnHandleClass;
    private final Function<ColumnHandle, Optional<R>> callback;

    public ColumnRule(Class<?> columnHandleClass, Function<ColumnHandle, Optional<R>> callback)
    {
        checkArgument(ColumnHandle.class.isAssignableFrom(columnHandleClass), "Must be subclass of columnHandle");
        this.columnHandleClass = requireNonNull(columnHandleClass);
        this.callback = callback;
    }

    public boolean match(RowExpression rowExpression)
    {
        if (rowExpression instanceof ColumnReferenceExpression) {
            return columnHandleClass.isInstance(((ColumnReferenceExpression) rowExpression).getColumnHandle());
        }
        return false;
    }

    public Optional<R> apply(RowExpression rowExpression)
    {
        checkArgument(rowExpression instanceof ColumnReferenceExpression, "rowExpression must be ColumnReferenceExpression");
        return callback.apply(((ColumnReferenceExpression) rowExpression).getColumnHandle());
    }

    public static class ListBuilder<R>
    {
        private final ImmutableList.Builder<ColumnRule<R>> builder = ImmutableList.builder();

        private static ListBuilder<?> builder()
        {
            return new ListBuilder<>();
        }

        public ListBuilder<R> add(Class<?> columnHandleClass, Function<ColumnHandle, Optional<R>> callback)
        {
            builder.add(new ColumnRule<>(columnHandleClass, callback));
            return this;
        }

        public List<ColumnRule<R>> build()
        {
            return builder.build();
        }
    }
}
