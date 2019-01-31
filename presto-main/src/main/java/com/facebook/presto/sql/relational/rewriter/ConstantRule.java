package com.facebook.presto.sql.relational.rewriter;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
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
            return ((ConstantExpression) rowExpression).getType().equals(type);
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
