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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.concurrent.Immutable;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

@Immutable
public final class Partitioning
{
    private final PartitioningHandle handle;
    private final List<RowExpression> arguments;

    private Partitioning(PartitioningHandle handle, List<RowExpression> arguments)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
    }

    public static Partitioning create(PartitioningHandle handle, List<Symbol> columns, Map<Symbol, Type> types)
    {
        return new Partitioning(handle, columns.stream()
                .map(symbol -> new VariableReferenceExpression(symbol.getName(), types.get(symbol)))
                .collect(toImmutableList()));
    }

    public static Partitioning createWithExpressions(PartitioningHandle handle, List<RowExpression> expressions)
    {
        return new Partitioning(handle, expressions.stream()
                .collect(toImmutableList()));
    }

    // Factory method for JSON serde only!
    @JsonCreator
    public static Partitioning jsonCreate(
            @JsonProperty("handle") PartitioningHandle handle,
            @JsonProperty("arguments") List<RowExpression> arguments)
    {
        return new Partitioning(handle, arguments);
    }

    @JsonProperty
    public PartitioningHandle getHandle()
    {
        return handle;
    }

    @JsonProperty
    public List<RowExpression> getArguments()
    {
        return arguments;
    }

    public Set<Symbol> getColumns()
    {
        return arguments.stream()
                .filter(VariableReferenceExpression.class::isInstance)
                .map(variable -> new Symbol(((VariableReferenceExpression) variable).getName()))
                .collect(toImmutableSet());
    }

    public boolean isCompatibleWith(
            Partitioning right,
            Metadata metadata,
            Session session)
    {
        if (!handle.equals(right.handle) && !metadata.getCommonPartitioning(session, handle, right.handle).isPresent()) {
            return false;
        }

        return arguments.equals(right.arguments);
    }

    public boolean isCompatibleWith(
            Partitioning right,
            Function<Symbol, Set<Symbol>> leftToRightMappings,
            Function<Symbol, Optional<NullableValue>> leftConstantMapping,
            Function<Symbol, Optional<NullableValue>> rightConstantMapping,
            Metadata metadata,
            Session session)
    {
        if (!handle.equals(right.handle) && !metadata.getCommonPartitioning(session, handle, right.handle).isPresent()) {
            return false;
        }

        if (arguments.size() != right.arguments.size()) {
            return false;
        }

        for (int i = 0; i < arguments.size(); i++) {
            RowExpression leftArgument = arguments.get(i);
            RowExpression rightArgument = right.arguments.get(i);

            if (!isPartitionedWith(leftArgument, leftConstantMapping, rightArgument, rightConstantMapping, leftToRightMappings)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isPartitionedWith(
            RowExpression leftArgument,
            Function<Symbol, Optional<NullableValue>> leftConstantMapping,
            RowExpression rightArgument,
            Function<Symbol, Optional<NullableValue>> rightConstantMapping,
            Function<Symbol, Set<Symbol>> leftToRightMappings)
    {
        if (leftArgument instanceof VariableReferenceExpression) {
            if (rightArgument instanceof VariableReferenceExpression) {
                // symbol == symbol
                Set<Symbol> mappedColumns = leftToRightMappings.apply(new Symbol(((VariableReferenceExpression) leftArgument).getName()));
                return mappedColumns.contains(new Symbol(((VariableReferenceExpression) rightArgument).getName()));
            }
            else if (leftArgument instanceof ConstantExpression) {
                // symbol == constant
                // Normally, this would be a false condition, but if we happen to have an external
                // mapping from the symbol to a constant value and that constant value matches the
                // right value, then we are co-partitioned.
                Optional<NullableValue> leftConstant = leftConstantMapping.apply(new Symbol(((VariableReferenceExpression) leftArgument).getName()));
                return leftConstant.isPresent() && leftConstant.get().equals(((ConstantExpression) leftArgument).getValue());
            }
        }
        else if (leftArgument instanceof ConstantExpression) {
            if (rightArgument instanceof ConstantExpression) {
                // constant == constant
                return leftArgument.equals(rightArgument);
            }
            else if (rightArgument instanceof VariableReferenceExpression) {
                // constant == symbol
                Optional<NullableValue> rightConstant = rightConstantMapping.apply(new Symbol(((VariableReferenceExpression) rightArgument).getName()));
                return rightConstant.isPresent() && rightConstant.get().getValue().equals(((ConstantExpression) leftArgument).getValue());
            }
        }
        throw new UnsupportedOperationException();
    }

    public boolean isPartitionedOn(Collection<Symbol> columns, Set<Symbol> knownConstants)
    {
        for (RowExpression argument : arguments) {
            // partitioned on (k_1, k_2, ..., k_n) => partitioned on (k_1, k_2, ..., k_n, k_n+1, ...)
            // can safely ignore all constant columns when comparing partition properties
            if (argument instanceof ConstantExpression) {
                continue;
            }
            if (!(argument instanceof VariableReferenceExpression)) {
                return false;
            }
            Symbol symbol = new Symbol(((VariableReferenceExpression) argument).getName());
            if (!knownConstants.contains(symbol) && !columns.contains(symbol)) {
                return false;
            }
        }
        return true;
    }

    public boolean isEffectivelySinglePartition(Set<Symbol> knownConstants)
    {
        return isPartitionedOn(ImmutableSet.of(), knownConstants);
    }

    public boolean isRepartitionEffective(Collection<Symbol> keys, Set<Symbol> knownConstants)
    {
        Set<Symbol> keysWithoutConstants = keys.stream()
                .filter(symbol -> !knownConstants.contains(symbol))
                .collect(toImmutableSet());
        Set<Symbol> nonConstantArgs = arguments.stream()
                .filter(VariableReferenceExpression.class::isInstance)
                .map(VariableReferenceExpression.class::cast)
                .map(VariableReferenceExpression::getName)
                .map(Symbol::new)
                .filter(symbol -> !knownConstants.contains(symbol))
                .collect(toImmutableSet());
        return !nonConstantArgs.equals(keysWithoutConstants);
    }

    public Partitioning translate(Function<Symbol, Symbol> translator)
    {
        return new Partitioning(handle, arguments.stream()
                .map(argument -> translate(argument, translator))
                .collect(toImmutableList()));
    }

    public Optional<Partitioning> translate(Translator translator)
    {
        ImmutableList.Builder<RowExpression> newArguments = ImmutableList.builder();
        for (RowExpression argument : arguments) {
            Optional<RowExpression> newArgument = translate(argument, translator);
            if (!newArgument.isPresent()) {
                return Optional.empty();
            }
            newArguments.add(newArgument.get());
        }

        return Optional.of(new Partitioning(handle, newArguments.build()));
    }

    public Partitioning withAlternativePartitiongingHandle(PartitioningHandle partitiongingHandle)
    {
        return new Partitioning(partitiongingHandle, this.arguments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(handle, arguments);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final Partitioning other = (Partitioning) obj;
        return Objects.equals(this.handle, other.handle) &&
                Objects.equals(this.arguments, other.arguments);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("handle", handle)
                .add("arguments", arguments)
                .toString();
    }

    public static Symbol getSymbol(RowExpression rowExpression)
    {
        checkArgument(rowExpression instanceof VariableReferenceExpression, "rowExpression must be variableReference");
        return new Symbol(((VariableReferenceExpression) rowExpression).getName());
    }

    @Immutable
    public static final class Translator
    {
        private final Function<Symbol, Optional<Symbol>> columnTranslator;
        private final Function<Symbol, Optional<NullableValue>> constantTranslator;
        private final Function<RowExpression, Optional<Symbol>> expressionTranslator;

        public Translator(
                Function<Symbol, Optional<Symbol>> columnTranslator,
                Function<Symbol, Optional<NullableValue>> constantTranslator,
                Function<RowExpression, Optional<Symbol>> expressionTranslator)
        {
            this.columnTranslator = requireNonNull(columnTranslator, "columnTranslator is null");
            this.constantTranslator = requireNonNull(constantTranslator, "constantTranslator is null");
            this.expressionTranslator = requireNonNull(expressionTranslator, "expressionTranslator is null");
        }
    }

    public RowExpression translate(RowExpression rowExpression, Function<Symbol, Symbol> translator)
    {
        if (isConstant(rowExpression)) {
            return rowExpression;
        }
        Symbol name = new Symbol(((VariableReferenceExpression) rowExpression).getName());
        return new VariableReferenceExpression(translator.apply(name).getName(), rowExpression.getType());
    }

    public Optional<RowExpression> translate(RowExpression rowExpression, Translator translator)
    {
        if (isConstant(rowExpression)) {
            return Optional.of(rowExpression);
        }

        if (!isSymbolReference(rowExpression)) {
            return translator.expressionTranslator.apply(rowExpression)
                    .map(symbol -> new VariableReferenceExpression(symbol.getName(), rowExpression.getType()));
        }

        Symbol name = new Symbol(((VariableReferenceExpression) rowExpression).getName());
        Optional<RowExpression> newColumn = translator.columnTranslator.apply(name)
                .map(symbol -> new VariableReferenceExpression(symbol.getName(), rowExpression.getType()));
        if (newColumn.isPresent()) {
            return newColumn;
        }
        // As a last resort, check for a constant mapping for the symbol
        // Note: this MUST be last because we want to favor the symbol representation
        // as it makes further optimizations possible.
        return translator.constantTranslator.apply(name)
                .map(value -> new ConstantExpression(value.getValue(), value.getType()));
    }

    public static boolean isSymbolReference(RowExpression rowExpression)
    {
        return rowExpression instanceof VariableReferenceExpression;
    }

    public static boolean isConstant(RowExpression rowExpression)
    {
        return rowExpression instanceof ConstantExpression && ((ConstantExpression) rowExpression).getValue() != null;
    }

    public static List<RowExpression> toArgumentBinding(List<Symbol> symbols, Map<Symbol, Type> types)
    {
        return symbols.stream()
                .map(symbol -> new VariableReferenceExpression(symbol.getName(), types.get(symbol)))
                .collect(toImmutableList());
    }
}
