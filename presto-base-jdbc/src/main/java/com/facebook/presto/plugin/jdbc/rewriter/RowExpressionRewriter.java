package com.facebook.presto.plugin.jdbc.rewriter;

import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ColumnReferenceExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

public class RowExpressionRewriter<R>

{
    private final List<FunctionRule<R>> functionRules;
    private final List<ConstantRule<R>> constantRules;
    private final List<ColumnRule<R>> columnRules;
    private final Optional<BiFunction<RowExpressionRewriter<R>, RowExpression, Optional<R>>> defaultRule;

    public RowExpressionRewriter(
            List<FunctionRule<R>> functionRules,
            List<ConstantRule<R>> constantRules,
            List<ColumnRule<R>> columnRules,
            Optional<BiFunction<RowExpressionRewriter<R>, RowExpression, Optional<R>>> defaultRule)
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

    public RowExpressionRewriter(BiFunction<RowExpressionRewriter<R>, RowExpression, Optional<R>> defaultRule)
    {
        this(ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), Optional.of(defaultRule));
    }

    public Optional<R> rewrite(RowExpression root)
    {
        return root.accept(new Visitor(this), null);
    }

    public class Visitor
            implements RowExpressionVisitor<Optional<R>, Void>
    {
        private final RowExpressionRewriter<R> rewriter;

        public Visitor(RowExpressionRewriter<R> rewriter)
        {
            this.rewriter = rewriter;
        }

        @Override
        public Optional<R> visitCall(CallExpression call, Void context)
        {
            Optional<FunctionRule<R>> matchedRule = functionRules.stream()
                    .filter(rule -> rule.match(call))
                    .findAny();
            if (!matchedRule.isPresent() && defaultRule.isPresent()) {
                return defaultRule.get().apply(rewriter, call);
            }
            return matchedRule.flatMap(rule -> rule.call(rewriter, call));
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
            if (!matchedRule.isPresent() && defaultRule.isPresent()) {
                return defaultRule.get().apply(rewriter, literal);
            }
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
            if (!matchedRule.isPresent() && defaultRule.isPresent()) {
                return defaultRule.get().apply(rewriter, columnReferenceExpression);
            }
            return matchedRule.flatMap(rule -> rule.apply(columnReferenceExpression));
        }
    }
}
