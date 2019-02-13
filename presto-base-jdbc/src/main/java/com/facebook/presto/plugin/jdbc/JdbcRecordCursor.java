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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.VerifyException;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class JdbcRecordCursor
        implements RecordCursor
{
    private static final Logger log = Logger.get(JdbcRecordCursor.class);

    private final JdbcColumnHandle[] columnHandles;
    private final int[] columnOffset;
    private final BooleanReadFunction[] booleanReadFunctions;
    private final DoubleReadFunction[] doubleReadFunctions;
    private final LongReadFunction[] longReadFunctions;
    private final SliceReadFunction[] sliceReadFunctions;

    private final JdbcClient jdbcClient;
    private final Connection connection;
    private final PreparedStatement statement;
    private final ResultSet resultSet;
    private boolean closed;

    public JdbcRecordCursor(JdbcClient jdbcClient, ConnectorSession session, JdbcSplit split, List<JdbcColumnHandle> columnHandles)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");

        this.columnHandles = columnHandles.toArray(new JdbcColumnHandle[0]);
        this.columnOffset = new int[columnHandles.size()];
        int offset = 0;
        for (int i = 0; i < columnHandles.size(); i++) {
            columnOffset[i] = offset;
            offset += columnHandles.get(i).getSqlCommand().isEmpty() ? 1 : columnHandles.get(i).getSqlCommand().size();
        }

        booleanReadFunctions = new BooleanReadFunction[offset];
        doubleReadFunctions = new DoubleReadFunction[offset];
        longReadFunctions = new LongReadFunction[offset];
        sliceReadFunctions = new SliceReadFunction[offset];

        for (int i = 0; i < this.columnHandles.length; i++) {
            for (int j = 0; j < columnHandles.get(i).getJdbcTypeHandle().size(); j++) {
                ReadMapping readMapping = jdbcClient.toPrestoType(session, columnHandles.get(i).getJdbcTypeHandle().get(j))
                        .orElseThrow(() -> new VerifyException("Unsupported column type"));
                Class<?> javaType = readMapping.getType().getJavaType();
                ReadFunction readFunction = readMapping.getReadFunction();

                if (javaType == boolean.class) {
                    booleanReadFunctions[i] = (BooleanReadFunction) readFunction;
                }
                else if (javaType == double.class) {
                    doubleReadFunctions[i] = (DoubleReadFunction) readFunction;
                }
                else if (javaType == long.class) {
                    longReadFunctions[i] = (LongReadFunction) readFunction;
                }
                else if (javaType == Slice.class) {
                    sliceReadFunctions[i] = (SliceReadFunction) readFunction;
                }
                else {
                    throw new IllegalStateException(format("Unsupported java type %s", javaType));
                }
            }
        }

        try {
            connection = jdbcClient.getConnection(split);
            statement = jdbcClient.buildSql(connection, split, columnHandles);
            log.info("Executing: %s", statement.toString());
            resultSet = statement.executeQuery();
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        return columnHandles[field].getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (closed) {
            return false;
        }

        try {
            return resultSet.next();
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkState(!closed, "cursor is closed");
        try {
            return booleanReadFunctions[field].readBoolean(resultSet, columnOffset[field] + 1);
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public boolean getBoolean(int field, int offset)
    {
        checkState(!closed, "cursor is closed");
        try {
            return booleanReadFunctions[field].readBoolean(resultSet, columnOffset[field] + offset + 1);
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public long getLong(int field)
    {
        checkState(!closed, "cursor is closed");
        try {
            return longReadFunctions[field].readLong(resultSet, columnOffset[field] + 1);
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public long getLong(int field, int offset)
    {
        checkState(!closed, "cursor is closed");
        try {
            long value = longReadFunctions[field].readLong(resultSet, columnOffset[field] + offset + 1);
            return value;
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public double getDouble(int field)
    {
        checkState(!closed, "cursor is closed");
        try {
            return doubleReadFunctions[field].readDouble(resultSet, columnOffset[field] + 1);
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public double getDouble(int field, int offset)
    {
        checkState(!closed, "cursor is closed");
        try {
            double value = doubleReadFunctions[field].readDouble(resultSet, columnOffset[field] + offset + 1);
            return value;
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public Slice getSlice(int field)
    {
        checkState(!closed, "cursor is closed");
        try {
            return sliceReadFunctions[field].readSlice(resultSet, columnOffset[field] + 1);
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public Slice getSlice(int field, int offset)
    {
        checkState(!closed, "cursor is closed");
        try {
            return sliceReadFunctions[field].readSlice(resultSet, columnOffset[field] + offset + 1);
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        checkState(!closed, "cursor is closed");
        checkArgument(field < columnHandles.length, "Invalid field index");

        try {
            // JDBC is kind of dumb: we need to read the field and then ask
            // if it was null, which means we are wasting effort here.
            // We could save the result of the field access if it matters.
            resultSet.getObject(columnOffset[field] + 1);

            return resultSet.wasNull();
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public boolean isNull(int field, int offset)
    {
        checkState(!closed, "cursor is closed");
        checkArgument(field < columnHandles.length, "Invalid field index");

        try {
            // JDBC is kind of dumb: we need to read the field and then ask
            // if it was null, which means we are wasting effort here.
            // We could save the result of the field access if it matters.
            resultSet.getObject(columnOffset[field] + offset + 1);

            return resultSet.wasNull();
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        // use try with resources to close everything properly
        try (Connection connection = this.connection;
                Statement statement = this.statement;
                ResultSet resultSet = this.resultSet) {
            jdbcClient.abortReadConnection(connection);
        }
        catch (SQLException e) {
            // ignore exception from close
        }
    }

    private RuntimeException handleSqlException(Exception e)
    {
        try {
            close();
        }
        catch (Exception closeException) {
            // Self-suppression not permitted
            if (e != closeException) {
                e.addSuppressed(closeException);
            }
        }
        return new PrestoException(JDBC_ERROR, e);
    }
}
