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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
<<<<<<< HEAD
import com.facebook.presto.spi.function.FunctionMetadata;
=======
>>>>>>> Introduce RowExpressionVerifier
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.LiteralInterpreter;
<<<<<<< HEAD
import com.facebook.presto.sql.relational.FunctionResolution;
=======
import com.facebook.presto.sql.relational.StandardFunctionResolution;
>>>>>>> Introduce RowExpressionVerifier
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.TryExpression;
import com.facebook.presto.sql.tree.WhenClause;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.function.OperatorType.ADD;
import static com.facebook.presto.spi.function.OperatorType.DIVIDE;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.MODULUS;
import static com.facebook.presto.spi.function.OperatorType.MULTIPLY;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.SUBTRACT;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.COALESCE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.SWITCH;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.WHEN;
import static com.facebook.presto.sql.planner.RowExpressionInterpreter.rowExpressionInterpreter;
<<<<<<< HEAD
=======
import static com.facebook.presto.sql.relational.StandardFunctionResolution.getArithmeticOperator;
import static com.facebook.presto.sql.relational.StandardFunctionResolution.getComparisonOperator;
import static com.facebook.presto.sql.relational.StandardFunctionResolution.isArithmeticFunction;
import static com.facebook.presto.sql.relational.StandardFunctionResolution.isComparisonFunction;
>>>>>>> Introduce RowExpressionVerifier
import static com.facebook.presto.sql.tree.LogicalBinaryExpression.Operator.AND;
import static com.facebook.presto.sql.tree.LogicalBinaryExpression.Operator.OR;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * RowExpression visitor which verifies if given expression (actual) is matching other RowExpression given as context (expected).
 */
final class RowExpressionVerifier
        extends AstVisitor<Boolean, RowExpression>
{
    // either use variable or input reference for symbol mapping
    private final SymbolAliases symbolAliases;
    private final Metadata metadata;
    private final Session session;
<<<<<<< HEAD
    private final FunctionResolution functionResolution;
=======
    private final StandardFunctionResolution functionResolution;
>>>>>>> Introduce RowExpressionVerifier

    RowExpressionVerifier(SymbolAliases symbolAliases, Metadata metadata, Session session)
    {
        this.symbolAliases = requireNonNull(symbolAliases, "symbolLayout is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.session = requireNonNull(session, "session is null");
<<<<<<< HEAD
        this.functionResolution = new FunctionResolution(metadata.getFunctionManager());
=======
        this.functionResolution = new StandardFunctionResolution(metadata.getFunctionManager());
>>>>>>> Introduce RowExpressionVerifier
    }

    @Override
    protected Boolean visitNode(Node node, RowExpression context)
    {
        throw new IllegalStateException(format("Node %s is not supported", node));
    }

    @Override
<<<<<<< HEAD
    protected Boolean visitTryExpression(TryExpression expected, RowExpression actual)
    {
        if (!(actual instanceof CallExpression) || !functionResolution.isTryFunction(((CallExpression) actual).getFunctionHandle())) {
            return false;
        }

        return process(expected.getInnerExpression(), ((CallExpression) actual).getArguments().get(0));
    }

    @Override
    protected Boolean visitCast(Cast expected, RowExpression actual)
    {
        // TODO: clean up cast path
        if (actual instanceof ConstantExpression && expected.getExpression() instanceof Literal && expected.getType().equals(actual.getType().toString())) {
            Literal literal = (Literal) expected.getExpression();
            if (literal instanceof StringLiteral) {
                Object value = LiteralInterpreter.evaluate(TEST_SESSION.toConnectorSession(), (ConstantExpression) actual);
                String actualString = value instanceof Slice ? ((Slice) value).toStringUtf8() : String.valueOf(value);
                return ((StringLiteral) literal).getValue().equals(actualString);
            }
            return getValueFromLiteral(literal).equals(String.valueOf(LiteralInterpreter.evaluate(TEST_SESSION.toConnectorSession(), (ConstantExpression) actual)));
        }
        if (actual instanceof VariableReferenceExpression && expected.getExpression() instanceof SymbolReference && expected.getType().equals(actual.getType().toString())) {
            return visitSymbolReference((SymbolReference) expected.getExpression(), actual);
        }
        if (!(actual instanceof CallExpression) || !functionResolution.isCastFunction(((CallExpression) actual).getFunctionHandle())) {
            return false;
        }

        if (!expected.getType().equalsIgnoreCase(actual.getType().toString())) {
            return false;
        }

        return process(expected.getExpression(), ((CallExpression) actual).getArguments().get(0));
    }

    @Override
    protected Boolean visitIsNullPredicate(IsNullPredicate expected, RowExpression actual)
    {
        if (!(actual instanceof SpecialFormExpression) || !((SpecialFormExpression) actual).getForm().equals(IS_NULL)) {
            return false;
        }

        return process(expected.getValue(), ((SpecialFormExpression) actual).getArguments().get(0));
    }

    @Override
    protected Boolean visitIsNotNullPredicate(IsNotNullPredicate expected, RowExpression actual)
    {
        if (!(actual instanceof CallExpression) || !functionResolution.notFunction().equals(((CallExpression) actual).getFunctionHandle())) {
            return false;
        }

        RowExpression argument = ((CallExpression) actual).getArguments().get(0);
=======
    protected Boolean visitTryExpression(TryExpression actual, RowExpression expected)
    {
        if (!(expected instanceof CallExpression) || !((CallExpression) expected).getFunctionHandle().getSignature().getName().equals("TRY")) {
            return false;
        }

        return process(actual.getInnerExpression(), ((CallExpression) expected).getArguments().get(0));
    }

    @Override
    protected Boolean visitCast(Cast actual, RowExpression expected)
    {
        // TODO: clean up cast path
        if (expected instanceof ConstantExpression && actual.getExpression() instanceof Literal && actual.getType().equals(expected.getType().toString())) {
            Literal literal = (Literal) actual.getExpression();
            if (literal instanceof StringLiteral) {
                Object value = LiteralInterpreter.evaluate(TEST_SESSION.toConnectorSession(), (ConstantExpression) expected);
                String expectedString = value instanceof Slice ? ((Slice) value).toStringUtf8() : String.valueOf(value);
                return ((StringLiteral) literal).getValue().equals(expectedString);
            }
            return getValueFromLiteral(literal).equals(String.valueOf(LiteralInterpreter.evaluate(TEST_SESSION.toConnectorSession(), (ConstantExpression) expected)));
        }
        if (expected instanceof VariableReferenceExpression && actual.getExpression() instanceof SymbolReference && actual.getType().equals(expected.getType().toString())) {
            return visitSymbolReference((SymbolReference) actual.getExpression(), expected);
        }
        if (!(expected instanceof CallExpression) || !functionResolution.isCastFunction(((CallExpression) expected).getFunctionHandle())) {
            return false;
        }

        if (!actual.getType().equals(expected.getType().toString())) {
            return false;
        }

        return process(actual.getExpression(), ((CallExpression) expected).getArguments().get(0));
    }

    @Override
    protected Boolean visitIsNullPredicate(IsNullPredicate actual, RowExpression expected)
    {
        if (!(expected instanceof SpecialFormExpression) || !((SpecialFormExpression) expected).getForm().equals(IS_NULL)) {
            return false;
        }

        return process(actual.getValue(), ((SpecialFormExpression) expected).getArguments().get(0));
    }

    @Override
    protected Boolean visitIsNotNullPredicate(IsNotNullPredicate actual, RowExpression expected)
    {
        if (!(expected instanceof CallExpression) || !functionResolution.notFunction().equals(((CallExpression) expected).getFunctionHandle())) {
            return false;
        }

        RowExpression argument = ((CallExpression) expected).getArguments().get(0);
>>>>>>> Introduce RowExpressionVerifier

        if (!(argument instanceof SpecialFormExpression) || !((SpecialFormExpression) argument).getForm().equals(IS_NULL)) {
            return false;
        }

<<<<<<< HEAD
        return process(expected.getValue(), ((SpecialFormExpression) argument).getArguments().get(0));
    }

    @Override
    protected Boolean visitInPredicate(InPredicate expected, RowExpression actual)
    {
        if (actual instanceof SpecialFormExpression && ((SpecialFormExpression) actual).getForm().equals(IN)) {
            List<RowExpression> arguments = ((SpecialFormExpression) actual).getArguments();
            if (expected.getValueList() instanceof InListExpression) {
                return process(expected.getValue(), arguments.get(0)) && process(((InListExpression) expected.getValueList()).getValues(), arguments.subList(1, arguments.size()));
            }
            else {
                /*
                 * If the actual value is a value list, but the expected is e.g. a SymbolReference,
                 * we need to unpack the value from the list so that when we hit visitSymbolReference, the
                 * actual.toString() call returns something that the symbolAliases expectedly contains.
                 * For example, InListExpression.toString returns "(onlyitem)" rather than "onlyitem".
                 *
                 * This is required because expected passes through the analyzer, planner, and possibly optimizers,
                 * one of which sometimes takes the liberty of unpacking the InListExpression.
                 *
                 * Since the actual value doesn't go through all of that, we have to deal with the case
                 * of the expected value being unpacked, but the actual value being an InListExpression.
                 */
                checkState(arguments.size() == 2, "Multiple expressions in actual value list %s, but expected value is not a list", arguments.subList(1, arguments.size()), expected.getValue());
                return process(expected.getValue(), arguments.get(0)) && process(expected.getValueList(), arguments.get(1));
=======
        return process(actual.getValue(), ((SpecialFormExpression) argument).getArguments().get(0));
    }

    @Override
    protected Boolean visitInPredicate(InPredicate actual, RowExpression expected)
    {
        if (expected instanceof SpecialFormExpression && ((SpecialFormExpression) expected).getForm().equals(IN)) {
            List<RowExpression> arguments = ((SpecialFormExpression) expected).getArguments();
            if (actual.getValueList() instanceof InListExpression) {
                return process(actual.getValue(), arguments.get(0)) && process(((InListExpression) actual.getValueList()).getValues(), arguments.subList(1, arguments.size()));
            }
            else {
                /*
                 * If the expected value is a value list, but the actual is e.g. a SymbolReference,
                 * we need to unpack the value from the list so that when we hit visitSymbolReference, the
                 * expected.toString() call returns something that the symbolAliases actually contains.
                 * For example, InListExpression.toString returns "(onlyitem)" rather than "onlyitem".
                 *
                 * This is required because actual passes through the analyzer, planner, and possibly optimizers,
                 * one of which sometimes takes the liberty of unpacking the InListExpression.
                 *
                 * Since the expected value doesn't go through all of that, we have to deal with the case
                 * of the actual value being unpacked, but the expected value being an InListExpression.
                 */
                checkState(arguments.size() == 2, "Multiple expressions in expected value list %s, but actual value is not a list", arguments.subList(1, arguments.size()), actual.getValue());
                return process(actual.getValue(), arguments.get(0)) && process(actual.getValueList(), arguments.get(1));
>>>>>>> Introduce RowExpressionVerifier
            }
        }
        return false;
    }

    @Override
<<<<<<< HEAD
    protected Boolean visitComparisonExpression(ComparisonExpression expected, RowExpression actual)
    {
        if (actual instanceof CallExpression) {
            FunctionMetadata functionMetadata = metadata.getFunctionManager().getFunctionMetadata(((CallExpression) actual).getFunctionHandle());
            if (!functionMetadata.getOperatorType().isPresent() || !functionMetadata.getOperatorType().get().isComparisonOperator()) {
                return false;
            }
            OperatorType actualOperatorType = functionMetadata.getOperatorType().get();
            OperatorType expectedOperatorType = getOperatorType(expected.getOperator());
            if (expectedOperatorType.equals(actualOperatorType)) {
                return process(expected.getLeft(), ((CallExpression) actual).getArguments().get(0)) && process(expected.getRight(), ((CallExpression) actual).getArguments().get(1));
=======
    protected Boolean visitComparisonExpression(ComparisonExpression actual, RowExpression expected)
    {
        if (expected instanceof CallExpression && isComparisonFunction(((CallExpression) expected).getFunctionHandle())) {
            OperatorType expectedOperatorType = getComparisonOperator((CallExpression) expected);
            OperatorType actualOperatorType = getOperatorType(actual.getOperator());
            if (actualOperatorType.equals(expectedOperatorType)) {
                return process(actual.getLeft(), ((CallExpression) expected).getArguments().get(0)) && process(actual.getRight(), ((CallExpression) expected).getArguments().get(1));
>>>>>>> Introduce RowExpressionVerifier
            }
        }
        return false;
    }

    private static OperatorType getOperatorType(ComparisonExpression.Operator operator)
    {
        OperatorType operatorType;
        switch (operator) {
            case EQUAL:
                operatorType = EQUAL;
                break;
            case NOT_EQUAL:
                operatorType = NOT_EQUAL;
                break;
            case LESS_THAN:
                operatorType = LESS_THAN;
                break;
            case LESS_THAN_OR_EQUAL:
                operatorType = LESS_THAN_OR_EQUAL;
                break;
            case GREATER_THAN:
                operatorType = GREATER_THAN;
                break;
            case GREATER_THAN_OR_EQUAL:
                operatorType = GREATER_THAN_OR_EQUAL;
                break;
            case IS_DISTINCT_FROM:
                operatorType = IS_DISTINCT_FROM;
                break;
            default:
                throw new IllegalStateException("Unsupported comparison operator type: " + operator);
        }
        return operatorType;
    }

    @Override
<<<<<<< HEAD
    protected Boolean visitArithmeticBinary(ArithmeticBinaryExpression expected, RowExpression actual)
    {
        if (actual instanceof CallExpression) {
            FunctionMetadata functionMetadata = metadata.getFunctionManager().getFunctionMetadata(((CallExpression) actual).getFunctionHandle());
            if (!functionMetadata.getOperatorType().isPresent() || !functionMetadata.getOperatorType().get().isArithmeticOperator()) {
                return false;
            }
            OperatorType actualOperatorType = functionMetadata.getOperatorType().get();
            OperatorType expectedOperatorType = getOperatorType(expected.getOperator());
            if (expectedOperatorType.equals(actualOperatorType)) {
                return process(expected.getLeft(), ((CallExpression) actual).getArguments().get(0)) && process(expected.getRight(), ((CallExpression) actual).getArguments().get(1));
=======
    protected Boolean visitArithmeticBinary(ArithmeticBinaryExpression actual, RowExpression expected)
    {
        if (expected instanceof CallExpression && isArithmeticFunction(((CallExpression) expected).getFunctionHandle())) {
            OperatorType expectedOperatorType = getArithmeticOperator((CallExpression) expected);
            OperatorType actualOperatorType = getOperatorType(actual.getOperator());
            if (actualOperatorType.equals(expectedOperatorType)) {
                return process(actual.getLeft(), ((CallExpression) expected).getArguments().get(0)) && process(actual.getRight(), ((CallExpression) expected).getArguments().get(1));
>>>>>>> Introduce RowExpressionVerifier
            }
        }
        return false;
    }

    private static OperatorType getOperatorType(ArithmeticBinaryExpression.Operator operator)
    {
        OperatorType operatorType;
        switch (operator) {
            case ADD:
                operatorType = ADD;
                break;
            case SUBTRACT:
                operatorType = SUBTRACT;
                break;
            case MULTIPLY:
                operatorType = MULTIPLY;
                break;
            case DIVIDE:
                operatorType = DIVIDE;
                break;
            case MODULUS:
                operatorType = MODULUS;
                break;
            default:
                throw new IllegalStateException("Unknown arithmetic operator: " + operator);
        }
        return operatorType;
    }

    @Override
<<<<<<< HEAD
    protected Boolean visitGenericLiteral(GenericLiteral expected, RowExpression actual)
    {
        return compareLiteral(expected, actual);
    }

    @Override
    protected Boolean visitLongLiteral(LongLiteral expected, RowExpression actual)
    {
        return compareLiteral(expected, actual);
    }

    @Override
    protected Boolean visitDoubleLiteral(DoubleLiteral expected, RowExpression actual)
    {
        return compareLiteral(expected, actual);
    }

    @Override
    protected Boolean visitDecimalLiteral(DecimalLiteral expected, RowExpression actual)
    {
        return compareLiteral(expected, actual);
    }

    @Override
    protected Boolean visitBooleanLiteral(BooleanLiteral expected, RowExpression actual)
    {
        return compareLiteral(expected, actual);
=======
    protected Boolean visitGenericLiteral(GenericLiteral actual, RowExpression expected)
    {
        return compareLiteral(actual, expected);
    }

    @Override
    protected Boolean visitLongLiteral(LongLiteral actual, RowExpression expected)
    {
        return compareLiteral(actual, expected);
    }

    @Override
    protected Boolean visitDoubleLiteral(DoubleLiteral actual, RowExpression expected)
    {
        return compareLiteral(actual, expected);
    }

    @Override
    protected Boolean visitDecimalLiteral(DecimalLiteral actual, RowExpression expected)
    {
        return compareLiteral(actual, expected);
    }

    @Override
    protected Boolean visitBooleanLiteral(BooleanLiteral actual, RowExpression expected)
    {
        return compareLiteral(actual, expected);
>>>>>>> Introduce RowExpressionVerifier
    }

    private static String getValueFromLiteral(Node expression)
    {
        if (expression instanceof LongLiteral) {
            return String.valueOf(((LongLiteral) expression).getValue());
        }
        else if (expression instanceof BooleanLiteral) {
            return String.valueOf(((BooleanLiteral) expression).getValue());
        }
        else if (expression instanceof DoubleLiteral) {
            return String.valueOf(((DoubleLiteral) expression).getValue());
        }
        else if (expression instanceof DecimalLiteral) {
            return String.valueOf(((DecimalLiteral) expression).getValue());
        }
        else if (expression instanceof GenericLiteral) {
            return ((GenericLiteral) expression).getValue();
        }
        else {
            throw new IllegalArgumentException("Unsupported literal expression type: " + expression.getClass().getName());
        }
    }

<<<<<<< HEAD
    private Boolean compareLiteral(Node expected, RowExpression actual)
    {
        if (actual instanceof CallExpression && functionResolution.isCastFunction(((CallExpression) actual).getFunctionHandle())) {
            return getValueFromLiteral(expected).equals(String.valueOf(rowExpressionInterpreter(actual, metadata, session.toConnectorSession()).evaluate()));
        }
        if (actual instanceof ConstantExpression) {
            return getValueFromLiteral(expected).equals(String.valueOf(LiteralInterpreter.evaluate(TEST_SESSION.toConnectorSession(), (ConstantExpression) actual)));
=======
    private Boolean compareLiteral(Node actual, RowExpression expected)
    {
        if (expected instanceof CallExpression && functionResolution.isCastFunction(((CallExpression) expected).getFunctionHandle())) {
            return getValueFromLiteral(actual).equals(String.valueOf(rowExpressionInterpreter(expected, metadata, session).evaluate()));
        }
        if (expected instanceof ConstantExpression) {
            return getValueFromLiteral(actual).equals(String.valueOf(LiteralInterpreter.evaluate(TEST_SESSION.toConnectorSession(), (ConstantExpression) expected)));
>>>>>>> Introduce RowExpressionVerifier
        }
        return false;
    }

    @Override
<<<<<<< HEAD
    protected Boolean visitStringLiteral(StringLiteral expected, RowExpression actual)
    {
        if (actual instanceof CallExpression && functionResolution.isCastFunction(((CallExpression) actual).getFunctionHandle())) {
            Object value = rowExpressionInterpreter(actual, metadata, session.toConnectorSession()).evaluate();
            if (value instanceof Slice) {
                return expected.getValue().equals(((Slice) value).toStringUtf8());
            }
        }
        if (actual instanceof ConstantExpression && actual.getType().getJavaType() == Slice.class) {
            String actualString = (String) LiteralInterpreter.evaluate(TEST_SESSION.toConnectorSession(), (ConstantExpression) actual);
            return expected.getValue().equals(actualString);
=======
    protected Boolean visitStringLiteral(StringLiteral actual, RowExpression expected)
    {
        if (expected instanceof ConstantExpression && expected.getType().getJavaType() == Slice.class) {
            String expectedString = (String) LiteralInterpreter.evaluate(TEST_SESSION.toConnectorSession(), (ConstantExpression) expected);
            return actual.getValue().equals(expectedString);
>>>>>>> Introduce RowExpressionVerifier
        }
        return false;
    }

    @Override
<<<<<<< HEAD
    protected Boolean visitLogicalBinaryExpression(LogicalBinaryExpression expected, RowExpression actual)
    {
        if (actual instanceof SpecialFormExpression) {
            SpecialFormExpression actualLogicalBinary = (SpecialFormExpression) actual;
            if ((expected.getOperator() == OR && actualLogicalBinary.getForm() == SpecialFormExpression.Form.OR) ||
                    (expected.getOperator() == AND && actualLogicalBinary.getForm() == SpecialFormExpression.Form.AND)) {
                return process(expected.getLeft(), actualLogicalBinary.getArguments().get(0)) &&
                        process(expected.getRight(), actualLogicalBinary.getArguments().get(1));
=======
    protected Boolean visitLogicalBinaryExpression(LogicalBinaryExpression actual, RowExpression expected)
    {
        if (expected instanceof SpecialFormExpression) {
            SpecialFormExpression expectedLogicalBinary = (SpecialFormExpression) expected;
            if ((actual.getOperator() == OR && expectedLogicalBinary.getForm() == SpecialFormExpression.Form.OR) ||
                    (actual.getOperator() == AND && expectedLogicalBinary.getForm() == SpecialFormExpression.Form.AND)) {
                return process(actual.getLeft(), expectedLogicalBinary.getArguments().get(0)) &&
                        process(actual.getRight(), expectedLogicalBinary.getArguments().get(1));
>>>>>>> Introduce RowExpressionVerifier
            }
        }
        return false;
    }

    @Override
<<<<<<< HEAD
    protected Boolean visitBetweenPredicate(BetweenPredicate expected, RowExpression actual)
    {
        if (actual instanceof CallExpression && functionResolution.isBetweenFunction(((CallExpression) actual).getFunctionHandle())) {
            return process(expected.getValue(), ((CallExpression) actual).getArguments().get(0)) &&
                    process(expected.getMin(), ((CallExpression) actual).getArguments().get(1)) &&
                    process(expected.getMax(), ((CallExpression) actual).getArguments().get(2));
=======
    protected Boolean visitBetweenPredicate(BetweenPredicate actual, RowExpression expected)
    {
        if (expected instanceof CallExpression && functionResolution.isBetweenFunction(((CallExpression) expected).getFunctionHandle())) {
            return process(actual.getValue(), ((CallExpression) expected).getArguments().get(0)) &&
                    process(actual.getMin(), ((CallExpression) expected).getArguments().get(1)) &&
                    process(actual.getMax(), ((CallExpression) expected).getArguments().get(2));
>>>>>>> Introduce RowExpressionVerifier
        }

        return false;
    }

    @Override
<<<<<<< HEAD
    protected Boolean visitNotExpression(NotExpression expected, RowExpression actual)
    {
        if (!(actual instanceof CallExpression) || !functionResolution.notFunction().equals(((CallExpression) actual).getFunctionHandle())) {
            return false;
        }
        return process(expected.getValue(), ((CallExpression) actual).getArguments().get(0));
    }

    @Override
    protected Boolean visitSymbolReference(SymbolReference expected, RowExpression actual)
    {
        if (!(actual instanceof VariableReferenceExpression)) {
            return false;
        }
        return symbolAliases.get((expected).getName()).getName().equals(((VariableReferenceExpression) actual).getName());
    }

    @Override
    protected Boolean visitCoalesceExpression(CoalesceExpression expected, RowExpression actual)
    {
        if (!(actual instanceof SpecialFormExpression) || !(((SpecialFormExpression) actual).getForm().equals(COALESCE))) {
            return false;
        }

        SpecialFormExpression actualCoalesce = (SpecialFormExpression) actual;
        if (expected.getOperands().size() == actualCoalesce.getArguments().size()) {
            boolean verified = true;
            for (int i = 0; i < expected.getOperands().size(); i++) {
                verified &= process(expected.getOperands().get(i), actualCoalesce.getArguments().get(i));
=======
    protected Boolean visitNotExpression(NotExpression actual, RowExpression expected)
    {
        if (!(expected instanceof CallExpression) || !functionResolution.notFunction().equals(((CallExpression) expected).getFunctionHandle())) {
            return false;
        }
        return process(actual.getValue(), ((CallExpression) expected).getArguments().get(0));
    }

    @Override
    protected Boolean visitSymbolReference(SymbolReference actual, RowExpression expected)
    {
        if (!(expected instanceof VariableReferenceExpression)) {
            return false;
        }
        return symbolAliases.get((actual).getName()).getName().equals(((VariableReferenceExpression) expected).getName());
    }

    @Override
    protected Boolean visitCoalesceExpression(CoalesceExpression actual, RowExpression expected)
    {
        if (!(expected instanceof SpecialFormExpression) || !(((SpecialFormExpression) expected).getForm().equals(COALESCE))) {
            return false;
        }

        SpecialFormExpression expectedCoalesce = (SpecialFormExpression) expected;
        if (actual.getOperands().size() == expectedCoalesce.getArguments().size()) {
            boolean verified = true;
            for (int i = 0; i < actual.getOperands().size(); i++) {
                verified &= process(actual.getOperands().get(i), expectedCoalesce.getArguments().get(i));
>>>>>>> Introduce RowExpressionVerifier
            }
            return verified;
        }
        return false;
    }

    @Override
<<<<<<< HEAD
    protected Boolean visitSimpleCaseExpression(SimpleCaseExpression expected, RowExpression actual)
    {
        if (!(actual instanceof SpecialFormExpression && ((SpecialFormExpression) actual).getForm().equals(SWITCH))) {
            return false;
        }
        SpecialFormExpression actualCase = (SpecialFormExpression) actual;
        if (!process(expected.getOperand(), actualCase.getArguments().get(0))) {
=======
    protected Boolean visitSimpleCaseExpression(SimpleCaseExpression actual, RowExpression expected)
    {
        if (!(expected instanceof SpecialFormExpression && ((SpecialFormExpression) expected).getForm().equals(SWITCH))) {
            return false;
        }
        SpecialFormExpression expectedCase = (SpecialFormExpression) expected;
        if (!process(actual.getOperand(), expectedCase.getArguments().get(0))) {
>>>>>>> Introduce RowExpressionVerifier
            return false;
        }

        List<RowExpression> whenClauses;
        Optional<RowExpression> elseValue;
<<<<<<< HEAD
        RowExpression last = actualCase.getArguments().get(actualCase.getArguments().size() - 1);
        if (last instanceof SpecialFormExpression && ((SpecialFormExpression) last).getForm().equals(WHEN)) {
            whenClauses = actualCase.getArguments().subList(1, actualCase.getArguments().size());
            elseValue = Optional.empty();
        }
        else {
            whenClauses = actualCase.getArguments().subList(1, actualCase.getArguments().size() - 1);
            elseValue = Optional.of(last);
        }

        if (!process(expected.getWhenClauses(), whenClauses)) {
            return false;
        }

        return process(expected.getDefaultValue(), elseValue);
    }

    @Override
    protected Boolean visitWhenClause(WhenClause expected, RowExpression actual)
    {
        if (!(actual instanceof SpecialFormExpression && ((SpecialFormExpression) actual).getForm().equals(WHEN))) {
            return false;
        }
        SpecialFormExpression actualWhenClause = (SpecialFormExpression) actual;

        return process(expected.getOperand(), ((SpecialFormExpression) actual).getArguments().get(0)) &&
                process(expected.getResult(), actualWhenClause.getArguments().get(1));
    }

    @Override
    protected Boolean visitFunctionCall(FunctionCall expected, RowExpression actual)
    {
        if (!(actual instanceof CallExpression)) {
            return false;
        }
        CallExpression actualFunction = (CallExpression) actual;

        if (!expected.getName().equals(QualifiedName.of(metadata.getFunctionManager().getFunctionMetadata(actualFunction.getFunctionHandle()).getName()))) {
            return false;
        }

        return process(expected.getArguments(), actualFunction.getArguments());
    }

    @Override
    protected Boolean visitNullLiteral(NullLiteral node, RowExpression actual)
    {
        return actual instanceof ConstantExpression && ((ConstantExpression) actual).getValue() == null;
    }

    private <T extends Node> boolean process(List<T> expecteds, List<RowExpression> actuals)
    {
        if (expecteds.size() != actuals.size()) {
            return false;
        }
        for (int i = 0; i < expecteds.size(); i++) {
            if (!process(expecteds.get(i), actuals.get(i))) {
=======
        RowExpression last = expectedCase.getArguments().get(expectedCase.getArguments().size() - 1);
        if (last instanceof SpecialFormExpression && ((SpecialFormExpression) last).getForm().equals(WHEN)) {
            whenClauses = expectedCase.getArguments().subList(1, expectedCase.getArguments().size());
            elseValue = Optional.empty();
        }
        else {
            whenClauses = expectedCase.getArguments().subList(1, expectedCase.getArguments().size() - 1);
            elseValue = Optional.of(last);
        }

        if (!process(actual.getWhenClauses(), whenClauses)) {
            return false;
        }

        return process(actual.getDefaultValue(), elseValue);
    }

    @Override
    protected Boolean visitWhenClause(WhenClause actual, RowExpression expected)
    {
        if (!(expected instanceof SpecialFormExpression && ((SpecialFormExpression) expected).getForm().equals(WHEN))) {
            return false;
        }
        SpecialFormExpression expectedWhenClause = (SpecialFormExpression) expected;

        return process(actual.getOperand(), ((SpecialFormExpression) expected).getArguments().get(0)) &&
                process(actual.getResult(), expectedWhenClause.getArguments().get(1));
    }

    @Override
    protected Boolean visitFunctionCall(FunctionCall actual, RowExpression expected)
    {
        if (!(expected instanceof CallExpression)) {
            return false;
        }
        CallExpression expectedFunction = (CallExpression) expected;

        if (!actual.getName().equals(QualifiedName.of(expectedFunction.getFunctionHandle().getSignature().getName()))) {
            return false;
        }

        return process(actual.getArguments(), expectedFunction.getArguments());
    }

    @Override
    protected Boolean visitNullLiteral(NullLiteral node, RowExpression expected)
    {
        return expected instanceof ConstantExpression && ((ConstantExpression) expected).getValue() == null;
    }

    private <T extends Node> boolean process(List<T> actuals, List<RowExpression> expecteds)
    {
        if (actuals.size() != expecteds.size()) {
            return false;
        }
        for (int i = 0; i < actuals.size(); i++) {
            if (!process(actuals.get(i), expecteds.get(i))) {
>>>>>>> Introduce RowExpressionVerifier
                return false;
            }
        }
        return true;
    }

<<<<<<< HEAD
    private <T extends Node> boolean process(Optional<T> expected, Optional<RowExpression> actual)
    {
        if (expected.isPresent() != actual.isPresent()) {
            return false;
        }
        if (expected.isPresent()) {
            return process(expected.get(), actual.get());
=======
    private <T extends Node> boolean process(Optional<T> actual, Optional<RowExpression> expected)
    {
        if (actual.isPresent() != expected.isPresent()) {
            return false;
        }
        if (actual.isPresent()) {
            return process(actual.get(), expected.get());
>>>>>>> Introduce RowExpressionVerifier
        }
        return true;
    }
}
