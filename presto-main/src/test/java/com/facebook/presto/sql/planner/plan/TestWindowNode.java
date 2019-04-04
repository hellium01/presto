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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.server.SliceDeserializer;
import com.facebook.presto.server.SliceSerializer;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.function.FunctionHandle;
<<<<<<< HEAD
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
=======
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
>>>>>>> Replace WindowNode::FunctionCall with CallExpression
import com.facebook.presto.sql.Serialization;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.OrderingScheme;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
<<<<<<< HEAD
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.type.TypeRegistry;
=======
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.type.TypeRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
>>>>>>> Replace WindowNode::FunctionCall with CallExpression
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonModule;
import io.airlift.slice.Slice;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType.UNBOUNDED_FOLLOWING;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType.UNBOUNDED_PRECEDING;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.WindowType.RANGE;
import static com.facebook.presto.sql.relational.Expressions.call;
<<<<<<< HEAD
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
=======
>>>>>>> Replace WindowNode::FunctionCall with CallExpression
import static org.testng.Assert.assertEquals;

public class TestWindowNode
{
    private PlanVariableAllocator variableAllocator;
    private ValuesNode sourceNode;
    private VariableReferenceExpression columnA;
    private VariableReferenceExpression columnB;
    private VariableReferenceExpression columnC;

    private final JsonCodec<WindowNode> codec;

    public TestWindowNode()
            throws Exception
    {
<<<<<<< HEAD
        codec = getJsonCodec();
=======
        // dependencies copied from ServerMainModule.java to avoid depending on whole ServerMainModule here
        SqlParser sqlParser = new SqlParser();
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setJsonSerializers(ImmutableMap.of(
                Slice.class, new SliceSerializer(),
                Expression.class, new Serialization.ExpressionSerializer()));
        provider.setJsonDeserializers(ImmutableMap.of(
                Slice.class, new SliceDeserializer(),
                Type.class, new TypeDeserializer(new TypeRegistry()),
                Expression.class, new Serialization.ExpressionDeserializer(sqlParser),
                FunctionCall.class, new Serialization.FunctionCallDeserializer(sqlParser)));
        objectMapper = provider.get();
>>>>>>> Replace WindowNode::FunctionCall with CallExpression
    }

    @BeforeClass
    public void setUp()
    {
        variableAllocator = new PlanVariableAllocator();
        columnA = variableAllocator.newVariable("a", BIGINT);
        columnB = variableAllocator.newVariable("b", BIGINT);
        columnC = variableAllocator.newVariable("c", BIGINT);

        sourceNode = new ValuesNode(
                newId(),
                ImmutableList.of(columnA, columnB, columnC),
                ImmutableList.of());
    }

    @Test
    public void testSerializationRoundtrip()
    {
<<<<<<< HEAD
        VariableReferenceExpression windowVariable = variableAllocator.newVariable("sum", BIGINT);
        FunctionHandle functionHandle = createTestMetadataManager().getFunctionManager().lookupFunction("sum", fromTypes(BIGINT));
=======
        Symbol windowSymbol = symbolAllocator.newSymbol("sum", BIGINT);
        FunctionHandle functionHandle = createTestMetadataManager().getFunctionManager().lookupFunction(QualifiedName.of("sum"), fromTypes(BIGINT));
>>>>>>> Replace WindowNode::FunctionCall with CallExpression
        WindowNode.Frame frame = new WindowNode.Frame(
                RANGE,
                UNBOUNDED_PRECEDING,
                Optional.empty(),
                UNBOUNDED_FOLLOWING,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        PlanNodeId id = newId();
        WindowNode.Specification specification = new WindowNode.Specification(
                ImmutableList.of(columnA),
                Optional.of(new OrderingScheme(
                        ImmutableList.of(columnB),
                        ImmutableMap.of(columnB, SortOrder.ASC_NULLS_FIRST))));
<<<<<<< HEAD
        CallExpression call = call("sum", functionHandle, BIGINT, new VariableReferenceExpression(columnC.getName(), BIGINT));
        Map<VariableReferenceExpression, WindowNode.Function> functions = ImmutableMap.of(windowVariable, new WindowNode.Function(call, frame));
        Optional<VariableReferenceExpression> hashVariable = Optional.of(columnB);
        Set<VariableReferenceExpression> prePartitionedInputs = ImmutableSet.of(columnA);
=======
        CallExpression call = call(functionHandle, BIGINT, new VariableReferenceExpression(columnC.getName(), BIGINT));
        Map<Symbol, WindowNode.Function> functions = ImmutableMap.of(windowSymbol, new WindowNode.Function(call, frame));
        Optional<Symbol> hashSymbol = Optional.of(columnB);
        Set<Symbol> prePartitionedInputs = ImmutableSet.of(columnA);
>>>>>>> Replace WindowNode::FunctionCall with CallExpression
        WindowNode windowNode = new WindowNode(
                id,
                sourceNode,
                specification,
                functions,
                hashVariable,
                prePartitionedInputs,
                0);

        String json = codec.toJson(windowNode);

        WindowNode actualNode = codec.fromJson(json);

        assertEquals(actualNode.getId(), windowNode.getId());
        assertEquals(actualNode.getSpecification(), windowNode.getSpecification());
        assertEquals(actualNode.getWindowFunctions(), windowNode.getWindowFunctions());
        assertEquals(actualNode.getFrames(), windowNode.getFrames());
        assertEquals(actualNode.getHashVariable(), windowNode.getHashVariable());
        assertEquals(actualNode.getPrePartitionedInputs(), windowNode.getPrePartitionedInputs());
        assertEquals(actualNode.getPreSortedOrderPrefix(), windowNode.getPreSortedOrderPrefix());
    }

    private static PlanNodeId newId()
    {
        return new PlanNodeId(UUID.randomUUID().toString());
    }

    private JsonCodec<WindowNode> getJsonCodec()
            throws Exception
    {
        Module module = binder -> {
            SqlParser sqlParser = new SqlParser();
            TypeManager typeManager = new TypeRegistry();
            binder.install(new JsonModule());
            binder.install(new HandleJsonModule());
            binder.bind(SqlParser.class).toInstance(sqlParser);
            binder.bind(TypeManager.class).toInstance(typeManager);
            configBinder(binder).bindConfig(FeaturesConfig.class);
            newSetBinder(binder, Type.class);
            jsonBinder(binder).addSerializerBinding(Slice.class).to(SliceSerializer.class);
            jsonBinder(binder).addDeserializerBinding(Slice.class).to(SliceDeserializer.class);
            jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
            jsonBinder(binder).addSerializerBinding(Expression.class).to(Serialization.ExpressionSerializer.class);
            jsonBinder(binder).addDeserializerBinding(Expression.class).to(Serialization.ExpressionDeserializer.class);
            jsonBinder(binder).addDeserializerBinding(FunctionCall.class).to(Serialization.FunctionCallDeserializer.class);
            jsonBinder(binder).addKeySerializerBinding(VariableReferenceExpression.class).to(Serialization.VariableReferenceExpressionSerializer.class);
            jsonBinder(binder).addKeyDeserializerBinding(VariableReferenceExpression.class).to(Serialization.VariableReferenceExpressionDeserializer.class);
            jsonCodecBinder(binder).bindJsonCodec(WindowNode.class);
        };
        Bootstrap app = new Bootstrap(ImmutableList.of(module));
        Injector injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .quiet()
                .initialize();
        return injector.getInstance(new Key<JsonCodec<WindowNode>>() {});
    }
}
