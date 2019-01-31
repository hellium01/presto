package com.facebook.presto.sql.relational.rewriter;

import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

public class FunctionRule<R>
        implements RewriteRule<R>
{
    private final FunctionPattern function;
    private final CallBack<RowExpressionRewriter<R>, List<RowExpression>, Optional<R>> callBack;

    public FunctionRule(FunctionPattern function, CallBack<RowExpressionRewriter<R>, List<RowExpression>, Optional<R>> callBack)
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

    public Optional<R> call(RowExpressionRewriter<R> rewriter, List<RowExpression> arguments)
    {
        return callBack.apply(rewriter, arguments);
    }

    public interface CallBack<I, J, R>
    {
        R apply(I a1, J a2);
    }

    public static class ListBuilder<R>
    {
        private final ImmutableList.Builder<FunctionRule<R>> builder = ImmutableList.builder();

        private static FunctionRule.ListBuilder<?> builder()
        {
            return new FunctionRule.ListBuilder<>();
        }

        public FunctionRule.ListBuilder<R> add(FunctionPattern function, CallBack<RowExpressionRewriter<R>, List<RowExpression>, Optional<R>> callback)
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
