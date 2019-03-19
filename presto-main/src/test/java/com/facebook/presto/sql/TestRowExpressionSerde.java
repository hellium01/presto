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
package com.facebook.presto.sql;

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.block.IntArrayBlock;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.Scope;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonModule;
import io.airlift.slice.Slice;
import io.airlift.stats.cardinality.HyperLogLog;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.ROW_CONSTRUCTOR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.lang.Float.floatToIntBits;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestRowExpressionSerde
{
    private final Metadata metadata = MetadataManager.createTestMetadataManager();
    private JsonCodec<RowExpression> codec;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        codec = getJsonCodec();
    }

    @Test
    public void testSimpleLiteral()
    {
        assertLiteral("TRUE", constant(true, BOOLEAN));
        assertLiteral("FALSE", constant(false, BOOLEAN));
        assertLiteral("CAST(NULL AS BOOLEAN)", constant(null, BOOLEAN));

        assertLiteral("TINYINT '1'", constant(1L, TINYINT));
        assertLiteral("SMALLINT '1'", constant(1L, SMALLINT));
        assertLiteral("1", constant(1L, INTEGER));
        assertLiteral("BIGINT '1'", constant(1L, BIGINT));

        assertLiteral("1.1", constant(1.1, DOUBLE));
        assertLiteral("nan()", constant(Double.NaN, DOUBLE));
        assertLiteral("infinity()", constant(Double.POSITIVE_INFINITY, DOUBLE));
        assertLiteral("-infinity()", constant(Double.NEGATIVE_INFINITY, DOUBLE));

        assertLiteral("CAST(1.1 AS REAL)", constant((long) floatToIntBits(1.1f), REAL));
        assertLiteral("CAST(nan() AS REAL)", constant((long) floatToIntBits(Float.NaN), REAL));
        assertLiteral("CAST(infinity() AS REAL)", constant((long) floatToIntBits(Float.POSITIVE_INFINITY), REAL));
        assertLiteral("CAST(-infinity() AS REAL)", constant((long) floatToIntBits(Float.NEGATIVE_INFINITY), REAL));

        assertStringLiteral("'String Literal'", "String Literal", VarcharType.createVarcharType(14));
        assertLiteral("CAST(NULL AS VARCHAR)", constant(null, VARCHAR));

        assertLiteral("DATE '1991-01-01'", constant(7670L, DATE));
        assertLiteral("TIMESTAMP '1991-01-01 00:00:00.000'", constant(662727600000L, TIMESTAMP));
    }

    @Test
    public void testArrayLiteral()
    {
        RowExpression rowExpression = getRoudTrip("ARRAY [1, 2, 3]");
        assertTrue(rowExpression instanceof ConstantExpression);
        Object value = ((ConstantExpression) rowExpression).getValue();
        assertTrue(value instanceof IntArrayBlock);
        IntArrayBlock block = (IntArrayBlock) value;
        assertEquals(block.getPositionCount(), 3);
        assertEquals(block.getInt(0, 0), 1);
        assertEquals(block.getInt(1, 0), 2);
        assertEquals(block.getInt(2, 0), 3);
    }

    @Test
    public void testRowLiteral()
    {
        assertEquals(getRoudTrip("ROW(1, 1.1)"),
                specialForm(
                        ROW_CONSTRUCTOR,
                        RowType.anonymous(
                                ImmutableList.of(
                                        INTEGER,
                                        DOUBLE)),
                        constant(1L, INTEGER),
                        constant(1.1, DOUBLE)));
    }

    @Test
    public void testHllLiteral()
    {
        RowExpression rowExpression = getRoudTrip("empty_approx_set()");
        assertTrue(rowExpression instanceof ConstantExpression);
        Object value = ((ConstantExpression) rowExpression).getValue();
        // Just check if we can deserialize the value since there is no way to compare hll
        HyperLogLog.newInstance((Slice) value);
    }

    @Test
    public void testUnserializableType()
    {
        assertThrowsWhenSerialize("CAST('$.a' AS JsonPath)");
    }

    private void assertThrowsWhenSerialize(@Language("SQL") String sql)
    {
        RowExpression rowExpression = translate(expression(sql, new ParsingOptions(AS_DOUBLE)));
        assertThrows(IllegalArgumentException.class, () -> codec.toJson(rowExpression));
    }

    private void assertLiteral(@Language("SQL") String sql, ConstantExpression expected)
    {
        assertEquals(getRoudTrip(sql), expected);
    }

    private void assertStringLiteral(@Language("SQL") String sql, String expectedString, Type expectedType)
    {
        RowExpression roundTrip = getRoudTrip(sql);
        assertTrue(roundTrip instanceof ConstantExpression);
        String roundTripValue = ((Slice) ((ConstantExpression) roundTrip).getValue()).toStringUtf8();
        Type roundTripType = roundTrip.getType();
        assertEquals(roundTripValue, expectedString);
        assertEquals(roundTripType, expectedType);
    }

    private RowExpression getRoudTrip(String sql)
    {
        RowExpression rowExpression = translate(expression(sql, new ParsingOptions(AS_DOUBLE)));
        String json = codec.toJson(rowExpression);
        return codec.fromJson(json);
    }

    private JsonCodec<RowExpression> getJsonCodec()
            throws Exception
    {
        Module module = new Module()
        {
            @Override
            public void configure(Binder binder)
            {
                binder.install(new JsonModule());
                configBinder(binder).bindConfig(FeaturesConfig.class);

                binder.bind(TypeRegistry.class).in(Scopes.SINGLETON);
                binder.bind(TypeManager.class).to(TypeRegistry.class).in(Scopes.SINGLETON);
                jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
                newSetBinder(binder, Type.class);

                binder.bind(BlockEncodingManager.class).in(Scopes.SINGLETON);
                binder.bind(BlockEncodingSerde.class).to(BlockEncodingManager.class).in(Scopes.SINGLETON);
                newSetBinder(binder, BlockEncoding.class);
                jsonBinder(binder).addSerializerBinding(Block.class).to(BlockJsonSerde.Serializer.class);
                jsonBinder(binder).addDeserializerBinding(Block.class).to(BlockJsonSerde.Deserializer.class);
                jsonCodecBinder(binder).bindJsonCodec(RowExpression.class);
            }
        };
        Bootstrap app = new Bootstrap(ImmutableList.of(module));
        Injector injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .quiet()
                .initialize();
        return injector.getInstance(new Key<JsonCodec<RowExpression>>() {});
    }

    private RowExpression translate(Expression expression)
    {
        return SqlToRowExpressionTranslator.translate(expression, getExpressionTypes(expression), metadata.getFunctionManager(), metadata.getTypeManager(), TEST_SESSION, true);
    }

    private Map<NodeRef<Expression>, Type> getExpressionTypes(Expression expression)
    {
        ExpressionAnalyzer expressionAnalyzer = ExpressionAnalyzer.createWithoutSubqueries(
                metadata.getFunctionManager(),
                metadata.getTypeManager(),
                TEST_SESSION,
                TypeProvider.empty(),
                emptyList(),
                node -> new IllegalStateException("Unexpected node: %s" + node),
                WarningCollector.NOOP,
                false);
        expressionAnalyzer.analyze(expression, Scope.create());
        return expressionAnalyzer.getExpressionTypes();
    }
}