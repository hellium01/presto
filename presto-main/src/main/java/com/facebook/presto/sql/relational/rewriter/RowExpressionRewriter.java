package com.facebook.presto.sql.relational.rewriter;

import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ColumnReferenceExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.LambdaDefinitionExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.facebook.presto.sql.relational.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

public class RowExpressionRewriter<R>
        implements RowExpressionVisitor<Optional<R>, Void>
{
    private final List<FunctionRule<R>> functionRules;
    private final List<ConstantRule<R>> constantRules;
    private final List<ColumnRule<R>> columnRules;
    private final Optional<CallBack<RowExpression, RowExpressionRewriter<R>, Optional<R>>> defaultRule;

    public RowExpressionRewriter(
            List<FunctionRule<R>> functionRules,
            List<ConstantRule<R>> constantRules,
            List<ColumnRule<R>> columnRules,
            Optional<CallBack<RowExpression, RowExpressionRewriter<R>, Optional<R>>> defaultRule)
    {
        this.functionRules = functionRules;
        this.constantRules = constantRules;
        this.columnRules = columnRules;
        this.defaultRule = defaultRule;
    }

    public RowExpressionRewriter(
            List<FunctionRule<R>> functionRules,
            List<ConstantRule<R>> constantRules,
            List<ColumnRule<R>> columnRules)
    {
        this(functionRules, constantRules, columnRules, Optional.empty());
    }

    public RowExpressionRewriter(CallBack<RowExpression, RowExpressionRewriter<R>, Optional<R>> defaultRule)
    {
        this(ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), Optional.of(defaultRule));
    }

    public Optional<R> rewrite(RowExpression root)
    {
        return root.accept(this, null);
    }

    @Override
    public Optional<R> visitCall(CallExpression call, Void context)
    {
        Optional<FunctionRule<R>> matchedRule = functionRules.stream()
                .filter(rule -> rule.match(call))
                .findAny();
        if (!matchedRule.isPresent() && defaultRule.isPresent()) {
            return defaultRule.get().apply(call, this);
        }
        return matchedRule.flatMap(rule -> rule.call(this, call.getArguments()));
    }

    @Override
    public Optional<R> visitInputReference(InputReferenceExpression reference, Void context)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<R> visitConstant(ConstantExpression literal, Void context)
    {
        Optional<ConstantRule<R>> matchedRule = constantRules.stream()
                .filter(rule -> rule.match(literal))
                .findAny();
        return matchedRule.flatMap(rule -> rule.call(literal.getValue()));
    }

    @Override
    public Optional<R> visitLambda(LambdaDefinitionExpression lambda, Void context)
    {
        return Optional.empty();
    }

    @Override
    public Optional<R> visitVariableReference(VariableReferenceExpression reference, Void context)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<R> visitColumnReference(ColumnReferenceExpression columnReferenceExpression, Void context)
    {
        Optional<ColumnRule<R>> matchedRule = columnRules.stream()
                .filter(rule -> rule.match(columnReferenceExpression))
                .findAny();
        return matchedRule.flatMap(rule -> rule.apply(columnReferenceExpression));
    }

    public interface CallBack<I, J, R>
    {
        R apply(I a1, J a2);
    }
}
