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
package com.facebook.presto.spi.relation;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class TableScan
        extends LeafNode
{
    private final ConnectorTableHandle tableHandle;
    private final List<RowExpression> output;
    private final Optional<ConnectorTransactionHandle> transactionHandle;
    private final Optional<ConnectorTableLayoutHandle> connectorTableLayoutHandle;

    public TableScan(
            ConnectorTableHandle tableHandle,
            List<RowExpression> output,
            Optional<ConnectorTransactionHandle> transactionHandle,
            Optional<ConnectorTableLayoutHandle> connectorTableLayoutHandle)
    {
        this.tableHandle = tableHandle;
        this.output = output;
        this.transactionHandle = transactionHandle;
        this.connectorTableLayoutHandle = connectorTableLayoutHandle;
    }

    public ConnectorTableHandle getTableHandle()
    {
        return tableHandle;
    }

    public List<RowExpression> getOutput()
    {
        return output;
    }

    public Optional<ConnectorTransactionHandle> getTransactionHandle()
    {
        return transactionHandle;
    }

    public Optional<ConnectorTableLayoutHandle> getConnectorTableLayoutHandle()
    {
        return connectorTableLayoutHandle;
    }

    public TableScan withTableLayout(ConnectorTableLayoutHandle connectorTableHandle)
    {
       return new TableScan(tableHandle, output, transactionHandle, Optional.of(connectorTableHandle));
    }

    @Override
    public <R, C> R accept(RelationVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableScan(this, context);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TableScan)) {
            return false;
        }
        TableScan tableScan = (TableScan) o;
        return Objects.equals(tableHandle, tableScan.tableHandle) &&
                Objects.equals(output, tableScan.output) &&
                Objects.equals(transactionHandle, tableScan.transactionHandle) &&
                Objects.equals(connectorTableLayoutHandle, tableScan.connectorTableLayoutHandle);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableHandle, output, transactionHandle, connectorTableLayoutHandle);
    }
}
