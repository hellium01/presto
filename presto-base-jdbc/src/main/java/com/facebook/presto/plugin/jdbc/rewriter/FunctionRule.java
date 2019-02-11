package com.facebook.presto.plugin.jdbc.rewriter;

import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

public class FunctionRule<R>
        implements RewriteRule<R>
{
    private final FunctionPattern function;
    private final BiFunction<RowExpressionRewriter<R>, CallExpression, R> callBack;

    public FunctionRule(FunctionPattern function, BiFunction<RowExpressionRewriter<R>, CallExpression, R> callBack)
    {
        this.function = function;
        this.callBack = callBack;
    }

    public boolean match(RowExpression rowExpression)
    {
        if (rowExpression instanceof CallExpression) {
            return function.matchFunction(((CallExpression) rowExpression).getSignature());
        }
        return false;
    }

    public Optional<R> call(RowExpressionRewriter<R> rewriter, CallExpression callExpression)
    {
        return Optional.ofNullable(callBack.apply(rewriter, callExpression));
    }

    public static class ListBuilder<R>
    {
        private final ImmutableList.Builder<FunctionRule<R>> builder = ImmutableList.builder();

        private static FunctionRule.ListBuilder<?> builder()
        {
            return new FunctionRule.ListBuilder<>();
        }

        public FunctionRule.ListBuilder<R> add(FunctionPattern function, BiFunction<RowExpressionRewriter<R>, CallExpression, R> callback)
        {
            builder.add(new FunctionRule<>(function, callback));
            return this;
        }

        public List<FunctionRule<R>> build()
        {
            return builder.build();
        }
    }
}
