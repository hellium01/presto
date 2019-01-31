package com.facebook.presto.sql.relational.rewriter;

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
