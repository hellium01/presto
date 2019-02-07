package com.facebook.presto.plugin.jdbc.rewriter;

import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;

public class FunctionPattern
{
    private final Optional<String> name;
    private final Optional<FunctionKind> kind;
    private final OptionalInt numOfArguments;
    private final List<Predicate<TypeSignature>> argementTypes;
    private final Optional<Predicate<TypeSignature>> returnType;
    private static final String OPERATOR_PREFIX = "$operator$";

    private FunctionPattern(
            Optional<String> name,
            Optional<FunctionKind> kind,
            OptionalInt numOfArguments, List<Predicate<TypeSignature>> argementTypes,
            Optional<Predicate<TypeSignature>> returnType)
    {
        this.name = name;
        this.kind = kind;
        this.numOfArguments = numOfArguments;
        this.argementTypes = argementTypes;
        this.returnType = returnType;
    }

    private FunctionPattern()
    {
        this(Optional.empty(), Optional.empty(), OptionalInt.empty(), ImmutableList.of(), Optional.empty());
    }

    public static FunctionPattern operator(OperatorType name)
    {
        return new FunctionPattern(Optional.of(mangleOperatorName(name)), Optional.empty(), OptionalInt.empty(), ImmutableList.of(), Optional.empty());
    }

    private static String mangleOperatorName(OperatorType name)
    {
        return OPERATOR_PREFIX + name.name();
    }

    public static FunctionPattern function(String name)
    {
        return new FunctionPattern(Optional.of(name), Optional.empty(), OptionalInt.empty(), ImmutableList.of(), Optional.empty());
    }

    public FunctionPattern onlyKind(FunctionKind kind)
    {
        return new FunctionPattern(name, Optional.of(kind), numOfArguments, argementTypes, returnType);
    }

    public FunctionPattern withNumberOfArgument(int numberOfArgument)
    {
        checkArgument(argementTypes.isEmpty(), "Cannot have predicate initialized first");
        return new FunctionPattern(name, kind, OptionalInt.of(numberOfArgument), argementTypes, returnType);
    }

    public FunctionPattern withArgumentTypeMatching(Predicate<TypeSignature>... predicates)
    {
        return withArgumentTypeMatching(ImmutableList.copyOf(predicates));
    }

    public FunctionPattern withArgumentTypeMatching(List<Predicate<TypeSignature>> predicates)
    {
        if (numOfArguments.isPresent()) {
            checkArgument(numOfArguments.getAsInt() >= predicates.size(), "predicate on argument must be less than number of argument");
            return new FunctionPattern(name, kind, numOfArguments, ImmutableList.copyOf(predicates), returnType);
        }
        return new FunctionPattern(name, kind, OptionalInt.of(argementTypes.size()), argementTypes, returnType);
    }

    public FunctionPattern returns(Type type)
    {
        return returns(typeSignature -> typeSignature.equals(type.getTypeSignature()));
    }

//    public FunctionPattern returns(Type... types)
//    {
//        List<Type> types = ImmutableList.copyOf(types);
//        return returns(typeSignature -> types.stream().map(type -> typeSignature.equals(type.getTypeSignature()));
//    }

    public FunctionPattern returns(Predicate<TypeSignature> returnTypePredicate)
    {
        return new FunctionPattern(name, kind, numOfArguments, argementTypes, Optional.of(returnTypePredicate));
    }

    public static FunctionPattern fromSignatures(List<Signature> functions)
    {
        return null;
    }

    public boolean matchFunction(Signature targetSignature)
    {
        if (name.isPresent() && !name.get().equalsIgnoreCase(targetSignature.getName())) {
            return false;
        }
        if (kind.isPresent() && !kind.get().equals(targetSignature.getKind())) {
            return false;
        }
        if (numOfArguments.isPresent() && numOfArguments.getAsInt() != targetSignature.getArgumentTypes().size()) {
            return false;
        }
        if (argementTypes.size() > 0) {
            if (targetSignature.getArgumentTypes().size() < argementTypes.size()) {
                return false;
            }
            boolean matched = true;
            for (int i = 0; i < argementTypes.size(); i++) {
                matched = matched && (argementTypes.get(i).test(targetSignature.getArgumentTypes().get(i)));
            }
            if (matched) {
                return false;
            }
        }
        if (returnType.isPresent() && !returnType.get().test(targetSignature.getReturnType())) {

            return false;
        }
        return true;
    }
}
