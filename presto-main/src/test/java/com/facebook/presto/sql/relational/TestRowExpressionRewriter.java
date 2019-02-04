package com.facebook.presto.sql.relational;

import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ColumnReferenceExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.analyzer.Scope;
import com.facebook.presto.sql.planner.LiteralEncoder;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.relational.rewriter.ColumnRule;
import com.facebook.presto.sql.relational.rewriter.ConstantRule;
import com.facebook.presto.sql.relational.rewriter.FunctionPattern;
import com.facebook.presto.sql.relational.rewriter.FunctionRule;
import com.facebook.presto.sql.relational.rewriter.RowExpressionRewriter;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.function.FunctionKind.AGGREGATE;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.function.OperatorType.ADD;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.sql.relational.rewriter.FunctionPattern.function;
import static com.facebook.presto.sql.relational.rewriter.FunctionPattern.operator;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toMap;
import static org.testng.Assert.assertTrue;

public class TestRowExpressionRewriter
{
    private final Metadata metadata = MetadataManager.createTestMetadataManager();
    private final LiteralEncoder literalEncoder = new LiteralEncoder(metadata.getBlockEncodingSerde());

    @Test
    public void testFunctionMatching()
    {
        FunctionPattern sum = function("avg");
        operator(ADD).returns(BIGINT);
        operator(ADD).returns(type -> type.equals(BIGINT.getTypeSignature()));

        sum.matchFunction(metadata.getFunctionRegistry(),
                new Signature("approx_distinct", AGGREGATE, BIGINT.getTypeSignature(), ImmutableList.of(BIGINT.getTypeSignature())));
        sum.matchFunction(metadata.getFunctionRegistry(),
                new Signature("avg", AGGREGATE, DOUBLE.getTypeSignature(), ImmutableList.of(BIGINT.getTypeSignature())));
    }

    @Test
    public void testRowExpressionConvertBack()
    {
        Expression expression = expression("c1 > BIGINT '100' AND (c1 + c2) > 100 AND hamming_distance(c3, 'test_str') > 10");
        Map<String, ColumnHandle> columns = ImmutableMap.of(
                "c1", new TestColumnHandle("c1", BIGINT),
                "c2", new TestColumnHandle("c2", BIGINT),
                "c3", new TestColumnHandle("c3", VARCHAR));
        Map<ColumnHandle, String> columnReverse = columns.entrySet()
                .stream()
                .collect(toMap(Map.Entry::getValue, Map.Entry::getKey));
        RowExpression rowExpression = translateAndOptimize2(expression, SCALAR,
                ImmutableMap.of(new Symbol("c1"), BIGINT, new Symbol("c2"), BIGINT, new Symbol("c3"), VARCHAR),
                columns);
        Optional<Expression> expression1 = RowExpressionToSqlTranslator.translate(rowExpression, ImmutableMap.of(), columnReverse, literalEncoder, metadata.getFunctionRegistry());
        assertTrue(expression1.isPresent());
    }

    @Test
    public void testTranslator()
    {

        RowExpression result = translateAndOptimize2(expression("avg(c1)-1"), AGGREGATE,
                ImmutableMap.of(new Symbol("c1"), BIGINT),
                ImmutableMap.of("c1", new TestColumnHandle("c1", BIGINT)));

        RowExpressionRewriter<List<String>> toSqlRewriter = new RowExpressionRewriter<List<String>>(
                new FunctionRule.ListBuilder<List<String>>()
                        .add(function("sum"), (currentRewriter, arguments) -> {
                            checkArgument(arguments.size() == 1);
                            Optional<List<String>> child = currentRewriter.rewrite(arguments.get(0));
                            if (!child.isPresent()) {
                                return Optional.empty();
                            }
                            return Optional.of(
                                    ImmutableList.of(String.format("sum(%s)", child.get().get(0))));
                        })
                        .add(function("avg"), (currentRewriter, arguments) -> {
                            checkArgument(arguments.size() == 1);
                            Optional<List<String>> child = currentRewriter.rewrite(arguments.get(0));
                            if (!child.isPresent()) {
                                return Optional.empty();
                            }
                            return Optional.of(
                                    ImmutableList.of(
                                            String.format("sum(%s)", child.get().get(0)),
                                            "count(*)"));
                        })
                        .add(operator(ADD), (currentRewriter, arguments) -> {
                            checkArgument(arguments.size() == 2);
                            Optional<List<String>> left = currentRewriter.rewrite(arguments.get(0));
                            Optional<List<String>> right = currentRewriter.rewrite(arguments.get(1));
                            if (left.isPresent() && right.isPresent()) {
                                return Optional.of(ImmutableList.of(String.format("(%s) + (%s)", left.get().get(0), right.get().get(0))));
                            }
                            return Optional.empty();
                        })
                        .build(),
                new ConstantRule.ListBuilder<List<String>>()
                        .add(BIGINT, object -> Optional.of(ImmutableList.of(String.format("%s", object))))
                        .add(INTEGER, object -> Optional.of(ImmutableList.of(String.format("%s", object))))
                        .build(),
                new ColumnRule.ListBuilder<List<String>>()
                        .add(TestColumnHandle.class, columnHandle -> Optional.of(ImmutableList.of(String.format("`%s`", ((TestColumnHandle) columnHandle).getColumnName()))))
                        .build());

        Optional<List<String>> rewritten = toSqlRewriter.rewrite(result);
        //Example 1:  avg(c1)
        //Example 2:  avg(c1+c2)
        //Example 3:  sum(c1)
        //Example 4:  date_trunc('day', c1) + interval '1' day
        //Example 5:  strlen
        RowExpressionRewriter<RowExpression> rewriter2 = new RowExpressionRewriter<RowExpression>((rowExpression, mywriter) -> {
            if (rowExpression instanceof CallExpression) {
                Optional<List<String>> sql = toSqlRewriter.rewrite(rowExpression);
                if (sql.isPresent()) {
                    return Optional.of(
                            new ColumnReferenceExpression(new TestColumnHandle(Joiner.on(',').join(sql.get()), rowExpression.getType()), rowExpression.getType()));
                    // if function is aggregation, it has to be root
                }
                CallExpression callExpression = (CallExpression) rowExpression;
                boolean anyChildRewritten = false;
                ImmutableList.Builder<RowExpression> newChildren = ImmutableList.builder();
                for (int i = 0; i < callExpression.getArguments().size(); i++) {
                    Optional<RowExpression> r = mywriter.rewrite(callExpression.getArguments().get(i));
                    if (r.isPresent()) {
                        anyChildRewritten = true;
                    }
                    newChildren.add(r.orElse(callExpression.getArguments().get(i)));
                }
                if (anyChildRewritten) {
                    return Optional.of(new CallExpression(callExpression.getSignature(), callExpression.getType(), newChildren.build()));
                }
            }
            return Optional.empty();
        });
        Optional<RowExpression> rowExpressionRewritten = rewriter2.rewrite(result);

        result = translateAndOptimize2(expression("c1 + c2 * c3"), SCALAR,
                ImmutableMap.of(new Symbol("c1"), BIGINT,
                        new Symbol("c2"), BIGINT, new Symbol("c3"), BIGINT),
                ImmutableMap.of("c1", new TestColumnHandle("c1", BIGINT),
                        "c2", new TestColumnHandle("c2", BIGINT),
                        "c3", new TestColumnHandle("c3", BIGINT)));
    }

    public static class TestColumnHandle
            implements ColumnHandle
    {
        private final String columnName;
        private final Type type;

        public TestColumnHandle(String columnName, Type type)
        {
            this.columnName = columnName;
            this.type = type;
        }

        public String getColumnName()
        {
            return columnName;
        }

        public Type getType()
        {
            return type;
        }
    }

    private RowExpression translateAndOptimize2(Expression expression, FunctionKind kind, Map<Symbol, Type> symbolTypeMap, Map<String, ColumnHandle> columns)
    {
        return translateAndOptimize(expression, kind, getExpressionTypes(expression, symbolTypeMap), columns);
    }

    private RowExpression translateAndOptimize(Expression expression, FunctionKind kind, Map<NodeRef<Expression>, Type> types, Map<String, ColumnHandle> columns)
    {
        return SqlToRowExpressionTranslator.translate(expression, kind, types, columns, metadata.getFunctionRegistry(), metadata.getTypeManager(), TEST_SESSION, false);
    }

    private Map<NodeRef<Expression>, Type> getExpressionTypes(Expression expression, Map<Symbol, Type> symbolTypes)
    {
        ExpressionAnalyzer expressionAnalyzer = ExpressionAnalyzer.createWithoutSubqueries(
                metadata.getFunctionRegistry(),
                metadata.getTypeManager(),
                TEST_SESSION,
                TypeProvider.copyOf(symbolTypes),
                emptyList(),
                node -> new IllegalStateException("Unexpected node: %s" + node),
                WarningCollector.NOOP,
                false);
        expressionAnalyzer.analyze(expression, Scope.create());
        return expressionAnalyzer.getExpressionTypes();
    }
}