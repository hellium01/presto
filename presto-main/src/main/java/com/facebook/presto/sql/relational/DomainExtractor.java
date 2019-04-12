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
package com.facebook.presto.sql.relational;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.OperatorNotFoundException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.Utils;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.InterpretedFunctionInvoker;
import com.facebook.presto.sql.planner.RowExpressionInterpreter;
import com.facebook.presto.sql.planner.optimizations.RowExpressionCanonicalizer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.metadata.CastType.CAST;
import static com.facebook.presto.metadata.CastType.SATURATED_FLOOR_CAST;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.spi.predicate.TupleDomain.columnWiseUnion;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.OR;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.LiteralEncoder.toRowExpression;
import static com.facebook.presto.sql.relational.DomainExtractor.ExtractionResult.resultOf;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.LogicalRowExpressions.TRUE;
import static com.facebook.presto.sql.relational.LogicalRowExpressions.and;
import static com.facebook.presto.sql.relational.LogicalRowExpressions.or;
import static com.facebook.presto.sql.relational.StandardFunctionResolution.getComparisonOperator;
import static com.facebook.presto.sql.relational.StandardFunctionResolution.isComparisonFunction;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.partitioningBy;

public class DomainExtractor
{
    private DomainExtractor()
    {
    }

    /**
     * Convert an Expression predicate into an ExtractionResult consisting of:
     * 1) A successfully extracted TupleDomain
     * 2) An Expression fragment which represents the part of the original Expression that will need to be re-evaluated
     * after filtering with the TupleDomain.
     */
    public static ExtractionResult fromPredicate(Metadata metadata, Session session, RowExpression predicate)
    {
        return predicate.accept(new Visitor(metadata, session), false);
    }

    private static class Visitor
            implements RowExpressionVisitor<ExtractionResult, Boolean>
    {
        private final InterpretedFunctionInvoker functionInvoker;
        private final Metadata metadata;
        private final Session session;
        private final FunctionManager functionManager;
        private final LogicalRowExpressions logicalRowExpressions;
        private final DeterminismEvaluator determinismEvaluator;
        private final StandardFunctionResolution resolution;
        private final RowExpressionCanonicalizer canonicalizer;

        private Visitor(Metadata metadata, Session session)
        {
            this.functionInvoker = new InterpretedFunctionInvoker(metadata.getFunctionManager());
            this.metadata = metadata;
            this.session = session;
            this.functionManager = metadata.getFunctionManager();
            this.logicalRowExpressions = new LogicalRowExpressions(functionManager);
            this.determinismEvaluator = new DeterminismEvaluator(functionManager);
            this.resolution = new StandardFunctionResolution(functionManager);
            this.canonicalizer = new RowExpressionCanonicalizer(functionManager);
        }

        @Override
        public ExtractionResult visitSpecialForm(SpecialFormExpression node, Boolean complement)
        {
            switch (node.getForm()) {
                case AND:
                case OR: {
                    if (node.getArguments().size() > 2) {
                        return logicalRowExpressions.combineConjuncts(node.getArguments()).accept(this, complement);
                    }
                    return visitBinaryLogic(node, complement);
                }
                case IN: {
                    RowExpression target = node.getArguments().get(0);
                    // optimize nodes first and split into constant list and those cannot be optimized into constant
                    Map<Boolean, List<RowExpression>> values = node.getArguments().subList(1, node.getArguments().size())
                            .stream()
                            .map(this::optimize)
                            .collect(partitioningBy(value -> value instanceof ConstantExpression && value.getType() == target.getType()));

                    // only contains constant
                    if (values.containsKey(true) && !values.isEmpty() && (!values.containsKey(false) || values.get(false).isEmpty())) {
                        // has to handle complement specifically here since complement of in should still not be null aware
                        ValueSet valueSet = ValueSet.copyOf(target.getType(), values.get(true)
                                .stream()
                                .map(value -> ((ConstantExpression) value).getValue())
                                .collect(toImmutableList()));
                        if (complement) {
                            valueSet = valueSet.complement();
                        }

                        return resultOf(target, Domain.create(valueSet, false));
                    }

                    ImmutableList.Builder<RowExpression> disjuncts = ImmutableList.builder();
                    int numDisjuncts = 0;
                    if (values.containsKey(true) && !values.get(true).isEmpty()) {
                        numDisjuncts++;
                        disjuncts.add(in(target, values.get(true)));
                    }
                    if (values.containsKey(false)) {
                        for (RowExpression expression : values.get(false)) {
                            numDisjuncts++;
                            disjuncts.add(call(functionManager.resolveOperator(EQUAL, fromTypes(target.getType(), expression.getType())), BOOLEAN, target, expression));
                        }
                    }
                    checkArgument(numDisjuncts > 0, "Cannot have empty in list");
                    ExtractionResult extractionResult = or(disjuncts.build()).accept(this, complement);

                    // preserve original IN predicate as remaining predicate
                    if (extractionResult.getTupleDomain().isAll()) {
                        RowExpression originalPredicate = node;
                        if (complement) {
                            originalPredicate = not(resolution, originalPredicate);
                        }
                        return resultOf(originalPredicate);
                    }
                    return extractionResult;
                }
                case IS_NULL: {
                    RowExpression target = node.getArguments().get(0);
                    Domain domain = complementIfNecessary(Domain.onlyNull(target.getType()), complement);
                    return resultOf(target, domain);
                }
                default:
                    return resultOf(complementIfNecessary(node, complement));
            }
        }

        @Override
        public ExtractionResult visitConstant(ConstantExpression node, Boolean complement)
        {
            if (node.getValue() == null) {
                return new ExtractionResult(TupleDomain.none(), TRUE);
            }
            if (node.getType() == BOOLEAN) {
                boolean value = complement != (boolean) node.getValue();
                return new ExtractionResult(value ? TupleDomain.all() : TupleDomain.none(), TRUE);
            }
            throw new IllegalStateException("Can not extract predicate from constant type: " + node.getType());
        }

        @Override
        public ExtractionResult visitLambda(LambdaDefinitionExpression node, Boolean complement)
        {
            return resultOf(complementIfNecessary(node, complement));
        }

        @Override
        public ExtractionResult visitVariableReference(VariableReferenceExpression node, Boolean complement)
        {
            return resultOf(complementIfNecessary(node, complement));
        }

        @Override
        public ExtractionResult visitCall(CallExpression node, Boolean complement)
        {
            if (node.getFunctionHandle().equals(resolution.notFunction())) {
                return node.getArguments().get(0).accept(this, !complement);
            }

            if (resolution.isBetweenFunction(node.getFunctionHandle())) {
                // Re-write as two comparison expressions
                return and(
                        binaryOperator(GREATER_THAN_OR_EQUAL, node.getArguments().get(0), node.getArguments().get(1)),
                        binaryOperator(LESS_THAN_OR_EQUAL, node.getArguments().get(0), node.getArguments().get(2))).accept(this, complement);
            }

            if (isComparisonFunction(node.getFunctionHandle())) {
                Optional<NormalizedSimpleComparison> optionalNormalized = toNormalizedSimpleComparison(getComparisonOperator(node), node.getArguments().get(0), node.getArguments().get(1));
                if (!optionalNormalized.isPresent()) {
                    return resultOf(complementIfNecessary(node, complement));
                }
                NormalizedSimpleComparison normalized = optionalNormalized.get();

                RowExpression expression = normalized.getExpression();
                if (expression instanceof CallExpression && resolution.isCastFunction(((CallExpression) expression).getFunctionHandle())) {
                    CallExpression castExpression = (CallExpression) expression;
                    if (!isImplicitCoercion(castExpression)) {
                        //
                        // we cannot use non-coercion cast to literal_type on symbol side to build tuple domain
                        //
                        // example which illustrates the problem:
                        //
                        // let t be of timestamp type:
                        //
                        // and expression be:
                        // cast(t as date) == date_literal
                        //
                        // after dropping cast we end up with:
                        //
                        // t == date_literal
                        //
                        // if we build tuple domain based coercion of date_literal to timestamp type we would
                        // end up with tuple domain with just one time point (cast(date_literal as timestamp).
                        // While we need range which maps to single date pointed by date_literal.
                        //
                        return resultOf(complementIfNecessary(node, complement));
                    }

                    CallExpression cast = (CallExpression) expression;
                    Type sourceType = cast.getArguments().get(0).getType();

                    // we use saturated floor cast value -> castSourceType to rewrite original expression to new one with one cast peeled off the symbol side
                    Optional<RowExpression> coercedExpression = coerceComparisonWithRounding(
                            sourceType, cast.getArguments().get(0), normalized.getValue(), normalized.getComparisonOperator());

                    if (coercedExpression.isPresent()) {
                        return coercedExpression.get().accept(this, complement);
                    }
                    return resultOf(complementIfNecessary(node, complement));
                }
                else {
                    ConstantExpression value = normalized.getValue();
                    Type type = value.getType(); // common type for symbol and value
                    return createComparisonExtractionResult(normalized.getComparisonOperator(), expression, type, value.getValue(), complement);
                }
            }

            return resultOf(complementIfNecessary(node, complement));
        }

        @Override
        public ExtractionResult visitInputReference(InputReferenceExpression node, Boolean complement)
        {
            return resultOf(complementIfNecessary(node, complement));
        }

        private Optional<RowExpression> coerceComparisonWithRounding(
                Type expressionType,
                RowExpression compareTarget,
                ConstantExpression constant,
                OperatorType comparisonOperator)
        {
            requireNonNull(constant, "nullableValue is null");
            if (constant.getValue() == null) {
                return Optional.empty();
            }
            Type valueType = constant.getType();
            Object value = constant.getValue();
            return floorValue(valueType, expressionType, value)
                    .map((floorValue) -> rewriteComparisonExpression(expressionType, compareTarget, valueType, value, floorValue, comparisonOperator));
        }

        private RowExpression rewriteComparisonExpression(
                Type expressionType,
                RowExpression compareTarget,
                Type valueType,
                Object originalValue,
                Object coercedValue,
                OperatorType comparisonOperator)
        {
            int originalComparedToCoerced = compareOriginalValueToCoerced(valueType, originalValue, expressionType, coercedValue);
            boolean coercedValueIsEqualToOriginal = originalComparedToCoerced == 0;
            boolean coercedValueIsLessThanOriginal = originalComparedToCoerced > 0;
            boolean coercedValueIsGreaterThanOriginal = originalComparedToCoerced < 0;
            RowExpression coercedLiteral = toRowExpression(coercedValue, expressionType);

            switch (comparisonOperator) {
                case GREATER_THAN_OR_EQUAL:
                case GREATER_THAN: {
                    if (coercedValueIsGreaterThanOriginal) {
                        return binaryOperator(GREATER_THAN_OR_EQUAL, compareTarget, coercedLiteral);
                    }
                    if (coercedValueIsEqualToOriginal) {
                        return binaryOperator(comparisonOperator, compareTarget, coercedLiteral);
                    }
                    return binaryOperator(GREATER_THAN, compareTarget, coercedLiteral);
                }
                case LESS_THAN_OR_EQUAL:
                case LESS_THAN: {
                    if (coercedValueIsLessThanOriginal) {
                        return binaryOperator(LESS_THAN_OR_EQUAL, compareTarget, coercedLiteral);
                    }
                    if (coercedValueIsEqualToOriginal) {
                        return binaryOperator(comparisonOperator, compareTarget, coercedLiteral);
                    }
                    return binaryOperator(LESS_THAN, compareTarget, coercedLiteral);
                }
                case EQUAL: {
                    if (coercedValueIsEqualToOriginal) {
                        return binaryOperator(EQUAL, compareTarget, coercedLiteral);
                    }
                    // Return something that is false for all non-null values
                    return and(binaryOperator(EQUAL, compareTarget, coercedLiteral),
                            binaryOperator(NOT_EQUAL, compareTarget, coercedLiteral));
                }
                case NOT_EQUAL: {
                    if (coercedValueIsEqualToOriginal) {
                        return binaryOperator(comparisonOperator, compareTarget, coercedLiteral);
                    }
                    // Return something that is true for all non-null values
                    return or(binaryOperator(EQUAL, compareTarget, coercedLiteral),
                            binaryOperator(NOT_EQUAL, compareTarget, coercedLiteral));
                }
                case IS_DISTINCT_FROM: {
                    if (coercedValueIsEqualToOriginal) {
                        return binaryOperator(comparisonOperator, compareTarget, coercedLiteral);
                    }
                    return TRUE;
                }
            }

            throw new IllegalArgumentException("Unhandled operator: " + comparisonOperator);
        }

        private RowExpression binaryOperator(OperatorType operatorType, RowExpression left, RowExpression right)
        {
            return call(
                    metadata.getFunctionManager().resolveOperator(operatorType, fromTypes(left.getType(), right.getType())),
                    BOOLEAN,
                    left,
                    right);
        }

        private Optional<Object> floorValue(Type fromType, Type toType, Object value)
        {
            return getSaturatedFloorCastOperator(fromType, toType)
                    .map((operator) -> functionInvoker.invoke(operator, session.toConnectorSession(), value));
        }

        private Optional<FunctionHandle> getSaturatedFloorCastOperator(Type fromType, Type toType)
        {
            try {
                return Optional.of(metadata.getFunctionManager().lookupCast(SATURATED_FLOOR_CAST, fromType.getTypeSignature(), toType.getTypeSignature()));
            }
            catch (OperatorNotFoundException e) {
                return Optional.empty();
            }
        }

        private int compareOriginalValueToCoerced(Type originalValueType, Object originalValue, Type coercedValueType, Object coercedValue)
        {
            FunctionHandle castToOriginalTypeOperator = metadata.getFunctionManager().lookupCast(CAST, coercedValueType.getTypeSignature(), originalValueType.getTypeSignature());
            Object coercedValueInOriginalType = functionInvoker.invoke(castToOriginalTypeOperator, session.toConnectorSession(), coercedValue);
            Block originalValueBlock = Utils.nativeValueToBlock(originalValueType, originalValue);
            Block coercedValueBlock = Utils.nativeValueToBlock(originalValueType, coercedValueInOriginalType);
            return originalValueType.compareTo(originalValueBlock, 0, coercedValueBlock, 0);
        }

        private boolean isImplicitCoercion(CallExpression cast)
        {
            Type sourceType = cast.getArguments().get(0).getType();
            Type targetType = cast.getType();
            return metadata.getTypeManager().canCoerce(sourceType, targetType);
        }

        private static Domain extractOrderableDomain(OperatorType comparisonOperator, Type type, Object value, boolean complement)
        {
            checkArgument(value != null);
            switch (comparisonOperator) {
                case EQUAL:
                    return Domain.create(complementIfNecessary(ValueSet.ofRanges(Range.equal(type, value)), complement), false);
                case GREATER_THAN:
                    return Domain.create(complementIfNecessary(ValueSet.ofRanges(Range.greaterThan(type, value)), complement), false);
                case GREATER_THAN_OR_EQUAL:
                    return Domain.create(complementIfNecessary(ValueSet.ofRanges(Range.greaterThanOrEqual(type, value)), complement), false);
                case LESS_THAN:
                    return Domain.create(complementIfNecessary(ValueSet.ofRanges(Range.lessThan(type, value)), complement), false);
                case LESS_THAN_OR_EQUAL:
                    return Domain.create(complementIfNecessary(ValueSet.ofRanges(Range.lessThanOrEqual(type, value)), complement), false);
                case NOT_EQUAL:
                    return Domain.create(complementIfNecessary(ValueSet.ofRanges(Range.lessThan(type, value), Range.greaterThan(type, value)), complement), false);
                case IS_DISTINCT_FROM:
                    // Need to potential complement the whole domain for IS_DISTINCT_FROM since it is null-aware
                    return complementIfNecessary(Domain.create(ValueSet.ofRanges(Range.lessThan(type, value), Range.greaterThan(type, value)), true), complement);
                default:
                    throw new AssertionError("Unhandled operator: " + comparisonOperator);
            }
        }

        private static Domain extractEquatableDomain(OperatorType comparisonOperator, Type type, Object value, boolean complement)
        {
            checkArgument(value != null);
            switch (comparisonOperator) {
                case EQUAL:
                    return Domain.create(complementIfNecessary(ValueSet.of(type, value), complement), false);
                case NOT_EQUAL:
                    return Domain.create(complementIfNecessary(ValueSet.of(type, value).complement(), complement), false);
                case IS_DISTINCT_FROM:
                    // Need to potential complement the whole domain for IS_DISTINCT_FROM since it is null-aware
                    return complementIfNecessary(Domain.create(ValueSet.of(type, value).complement(), true), complement);
                default:
                    throw new AssertionError("Unhandled operator: " + comparisonOperator);
            }
        }

        private RowExpression optimize(RowExpression input)
        {
            if (input instanceof ConstantExpression || input instanceof VariableReferenceExpression || input instanceof InputReferenceExpression) {
                return input;
            }
            Object result = new RowExpressionInterpreter(input, metadata, session, true).optimize();
            if (result instanceof RowExpression) {
                return canonicalizer.canonicalize((RowExpression) result);
            }
            return toRowExpression(result, input.getType());
        }

        /**
         * Extract a normalized simple comparison between a QualifiedNameReference and a native value if possible.
         */
        private Optional<NormalizedSimpleComparison> toNormalizedSimpleComparison(OperatorType operatorType, RowExpression leftExpression, RowExpression rightExpression)
        {
            RowExpression left = optimize(leftExpression);
            RowExpression right = optimize(rightExpression);

            if (right instanceof ConstantExpression) {
                return Optional.of(new NormalizedSimpleComparison(left, operatorType, (ConstantExpression) right));
            }
            else if (left instanceof ConstantExpression) {
                return Optional.of(new NormalizedSimpleComparison(right, flip(operatorType), (ConstantExpression) left));
            }

            // we expect one side to be row expression and other to be value.
            return Optional.empty();
        }

        private static ExtractionResult createComparisonExtractionResult(OperatorType comparisonOperator, RowExpression column, Type type, @Nullable Object value, boolean complement)
        {
            if (value == null) {
                switch (comparisonOperator) {
                    case EQUAL:
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                    case NOT_EQUAL:
                        return new ExtractionResult(TupleDomain.none(), TRUE);

                    case IS_DISTINCT_FROM:
                        Domain domain = complementIfNecessary(Domain.notNull(type), complement);
                        return resultOf(column, domain);

                    default:
                        throw new AssertionError("Unhandled operator: " + comparisonOperator);
                }
            }

            Domain domain;
            if (type.isOrderable()) {
                domain = extractOrderableDomain(comparisonOperator, type, value, complement);
            }
            else if (type.isComparable()) {
                domain = extractEquatableDomain(comparisonOperator, type, value, complement);
            }
            else {
                throw new AssertionError("Type cannot be used in a comparison expression (should have been caught in analysis): " + type);
            }

            return resultOf(column, domain);
        }

        private static OperatorType flip(OperatorType operatorType)
        {
            switch (operatorType) {
                case EQUAL:
                    return EQUAL;
                case NOT_EQUAL:
                    return NOT_EQUAL;
                case LESS_THAN:
                    return GREATER_THAN;
                case LESS_THAN_OR_EQUAL:
                    return GREATER_THAN_OR_EQUAL;
                case GREATER_THAN:
                    return LESS_THAN;
                case GREATER_THAN_OR_EQUAL:
                    return LESS_THAN_OR_EQUAL;
                case IS_DISTINCT_FROM:
                    return IS_DISTINCT_FROM;
                default:
                    throw new IllegalArgumentException("Unsupported comparison: " + operatorType);
            }
        }

        private static ValueSet complementIfNecessary(ValueSet valueSet, boolean complement)
        {
            return complement ? valueSet.complement() : valueSet;
        }

        private static Domain complementIfNecessary(Domain domain, boolean complement)
        {
            return complement ? domain.complement() : domain;
        }

        private RowExpression complementIfNecessary(RowExpression expression, boolean complement)
        {
            return complement ? not(resolution, expression) : expression;
        }

        private ExtractionResult visitBinaryLogic(SpecialFormExpression node, Boolean complement)
        {
            ExtractionResult leftResult = node.getArguments().get(0).accept(this, complement);
            ExtractionResult rightResult = node.getArguments().get(1).accept(this, complement);

            TupleDomain<RowExpression> leftTupleDomain = leftResult.getTupleDomain();
            TupleDomain<RowExpression> rightTupleDomain = rightResult.getTupleDomain();

            SpecialFormExpression.Form operator = node.getForm();
            if (complement) {
                if (operator == AND) {
                    operator = OR;
                }
                else if (operator == OR) {
                    operator = AND;
                }
                else {
                    throw new IllegalStateException("Can not extract predicate from special form: " + node.getForm());
                }
            }

            switch (operator) {
                case AND: {
                    return new ExtractionResult(
                            leftTupleDomain.intersect(rightTupleDomain),
                            logicalRowExpressions.combineConjuncts(leftResult.getRemainingExpression(), rightResult.getRemainingExpression()));
                }
                case OR: {
                    TupleDomain<RowExpression> columnUnionedTupleDomain = columnWiseUnion(leftTupleDomain, rightTupleDomain);

                    // In most rest cases, the columnUnionedTupleDomain is only a superset of the actual strict union
                    // and so we can return the current node as the remainingExpression so that all bounds will be double checked again at execution time.
                    RowExpression remainingExpression = complementIfNecessary(node, complement);

                    // However, there are a few cases where the column-wise union is actually equivalent to the strict union, so we if can detect
                    // some of these cases, we won't have to double check the bounds unnecessarily at execution time.

                    // We can only make inferences if the remaining expressions on both side are equal and deterministic
                    if (leftResult.getRemainingExpression().equals(rightResult.getRemainingExpression()) &&
                            determinismEvaluator.isDeterministic(leftResult.getRemainingExpression())) {
                        // The column-wise union is equivalent to the strict union if
                        // 1) If both TupleDomains consist of the same exact single column (e.g. left TupleDomain => (a > 0), right TupleDomain => (a < 10))
                        // 2) If one TupleDomain is a superset of the other (e.g. left TupleDomain => (a > 0, b > 0 && b < 10), right TupleDomain => (a > 5, b = 5))
                        boolean matchingSingleSymbolDomains = !leftTupleDomain.isNone()
                                && !rightTupleDomain.isNone()
                                && leftTupleDomain.getDomains().get().size() == 1
                                && rightTupleDomain.getDomains().get().size() == 1
                                && leftTupleDomain.getDomains().get().keySet().equals(rightTupleDomain.getDomains().get().keySet());
                        boolean oneSideIsSuperSet = leftTupleDomain.contains(rightTupleDomain) || rightTupleDomain.contains(leftTupleDomain);

                        if (matchingSingleSymbolDomains || oneSideIsSuperSet) {
                            remainingExpression = leftResult.getRemainingExpression();
                        }
                    }

                    return new ExtractionResult(columnUnionedTupleDomain, remainingExpression);
                }
                default:
                    throw new IllegalStateException("Can not extract predicate from special form: " + node.getForm());
            }
        }
    }

    private static RowExpression not(StandardFunctionResolution resolution, RowExpression expression)
    {
        return call(resolution.notFunction(), expression.getType(), expression);
    }

    private static RowExpression in(RowExpression value, List<RowExpression> inList)
    {
        return new SpecialFormExpression(IN, BOOLEAN, ImmutableList.<RowExpression>builder().add(value).addAll(inList).build());
    }

    private static class NormalizedSimpleComparison
    {
        private final RowExpression expression;
        private final OperatorType comparisonOperator;
        private final ConstantExpression value;

        public NormalizedSimpleComparison(RowExpression expression, OperatorType comparisonOperator, ConstantExpression value)
        {
            this.expression = requireNonNull(expression, "expression is null");
            this.comparisonOperator = requireNonNull(comparisonOperator, "comparisonOperator is null");
            this.value = requireNonNull(value, "value is null");
        }

        public RowExpression getExpression()
        {
            return expression;
        }

        public OperatorType getComparisonOperator()
        {
            return comparisonOperator;
        }

        public ConstantExpression getValue()
        {
            return value;
        }
    }

    public static class ExtractionResult
    {
        private final TupleDomain<RowExpression> extracted;
        private final RowExpression rest;

        public static ExtractionResult resultOf(RowExpression... results)
        {
            RowExpression rest = TRUE;
            if (results.length == 1) {
                rest = results[0];
            }
            else if (results.length > 1) {
                rest = new SpecialFormExpression(AND, BOOLEAN, results);
            }
            return new ExtractionResult(TupleDomain.all(), rest);
        }

        public static ExtractionResult resultOf(RowExpression key, Domain domain)
        {
            return new ExtractionResult(TupleDomain.withColumnDomains(ImmutableMap.of(key, domain)), TRUE);
        }

        public ExtractionResult(TupleDomain<RowExpression> tupleDomain, RowExpression rest)
        {
            this.extracted = requireNonNull(tupleDomain, "remainingExpression is null");
            this.rest = rest;
        }

        public TupleDomain<RowExpression> getTupleDomain()
        {
            return extracted;
        }

        public RowExpression getRemainingExpression()
        {
            return rest;
        }
    }
}
