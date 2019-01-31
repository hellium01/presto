package com.facebook.presto.metadata;

import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class InternalFunction
{
    private InternalFunction()
    {
    }

    public static Signature internalOperator(OperatorType operator, Type returnType, List<? extends Type> argumentTypes)
    {
        return internalScalarFunction(FunctionRegistry.mangleOperatorName(operator.name()), returnType.getTypeSignature(), argumentTypes.stream().map(Type::getTypeSignature).collect(ImmutableList.toImmutableList()));
    }

    public static Signature internalOperator(OperatorType operator, TypeSignature returnType, TypeSignature... argumentTypes)
    {
        return internalOperator(operator, returnType, ImmutableList.copyOf(argumentTypes));
    }

    public static Signature internalOperator(OperatorType operator, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        return internalScalarFunction(FunctionRegistry.mangleOperatorName(operator.name()), returnType, argumentTypes);
    }

    public static Signature internalOperator(String name, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        return internalScalarFunction(FunctionRegistry.mangleOperatorName(name), returnType, argumentTypes);
    }

    public static Signature internalOperator(String name, TypeSignature returnType, TypeSignature... argumentTypes)
    {
        return internalScalarFunction(FunctionRegistry.mangleOperatorName(name), returnType, ImmutableList.copyOf(argumentTypes));
    }

    public static Signature internalScalarFunction(String name, TypeSignature returnType, TypeSignature... argumentTypes)
    {
        return internalScalarFunction(name, returnType, ImmutableList.copyOf(argumentTypes));
    }

    public static Signature internalScalarFunction(String name, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        return new Signature(name, FunctionKind.SCALAR, ImmutableList.of(), ImmutableList.of(), returnType, argumentTypes, false);
    }

    public static SignatureBuilder builder()
    {
        return new SignatureBuilder();
    }
}
