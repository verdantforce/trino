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
package io.trino.plugin.google.sheets;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.statistics.ComputedStatistics;

import javax.inject.Inject;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.google.sheets.SheetsErrorCode.SHEETS_UNKNOWN_TABLE_ERROR;
import static java.util.Objects.requireNonNull;

public class SheetsMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(SheetsMetadata.class);
    private final SheetsClient sheetsClient;
    private static final List<String> SCHEMAS = ImmutableList.of("default");

    @Inject
    public SheetsMetadata(SheetsClient sheetsClient)
    {
        this.sheetsClient = requireNonNull(sheetsClient, "sheetsClient is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return listSchemaNames();
    }

    public List<String> listSchemaNames()
    {
        return SCHEMAS;
    }

    @Override
    public SheetsTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        log.info("getTableHandle Called!");
        requireNonNull(tableName, "tableName is null");
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }

        Optional<SheetsTable> table = sheetsClient.getTable(tableName.getTableName());
        if (table.isEmpty()) {
            return null;
        }

        SheetsTableHandle sheetsTableHandle = new SheetsTableHandle(tableName.getSchemaName(), tableName.getTableName(), table.get().getLocation());
        log.info("returning sheets table handle: %s", sheetsTableHandle);
        return sheetsTableHandle;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        SheetsTableHandle tableHandle = (SheetsTableHandle) table;
        return getTableMetadata(tableHandle.toSchemaTableName())
                .orElseThrow(() -> new TrinoException(SHEETS_UNKNOWN_TABLE_ERROR, "Metadata not found for table " + tableHandle.getTableName()));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        SheetsTableHandle sheetsTableHandle = (SheetsTableHandle) tableHandle;
        SheetsTable table = sheetsClient.getTable(sheetsTableHandle.getTableName())
                .orElseThrow(() -> new TableNotFoundException(sheetsTableHandle.toSchemaTableName()));

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        for (ColumnMetadata column : table.getColumnsMetadata()) {
            columnHandles.put(column.getName(), new SheetsColumnHandle(column.getName(), column.getType(), index));
            index++;
        }
        return columnHandles.buildOrThrow();
    }

    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableList.Builder<TableColumnsMetadata> metadata = ImmutableList.builder();

        for (SchemaTableName tableName : listTables(session, prefix.getSchema())) {
            Optional<ConnectorTableMetadata> tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata.isPresent()) {
                metadata.add(new TableColumnsMetadata(
                        tableName,
                        tableMetadata.map(ConnectorTableMetadata::getColumns)));
            }
        }
        return metadata.build().stream().iterator();
    }
//
//    /**
//     * Begin the atomic creation of a table with data.
//     * <p>
//     * <p/>
//     * If connector does not support execution with retries, the method should throw:
//     * <pre>
//     *     new TrinoException(NOT_SUPPORTED, "This connector does not support query retries")
//     * </pre>
//     * unless {@code retryMode} is set to {@code NO_RETRIES}.
//     *
//     * @param session
//     * @param tableMetadata
//     * @param layout
//     * @param retryMode
//     */
//    @Override
//    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode)
//    {
//        return ConnectorMetadata.super.beginCreateTable(session, tableMetadata, layout, retryMode);
//    }
//
//    /**
//     * Finish a table creation with data after the data is written.
//     *
//     * @param session
//     * @param tableHandle
//     * @param fragments
//     * @param computedStatistics
//     */
//    @Override
//    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
//    {
//        return ConnectorMetadata.super.finishCreateTable(session, tableHandle, fragments, computedStatistics);
//    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        List<SheetsColumnHandle> columnHandles = columns.stream().map(x -> (SheetsColumnHandle) x).toList();
//        log.info("beginInsert: %s", ((SheetsTableHandle) tableHandle).toString());
        return new SheetsInsertTableHandle(
                (SheetsTableHandle) tableHandle,
                columnHandles);
    }

//    @Override
//    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
//    {
//        requireNonNull(prefix, "prefix is null");
//        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
//        for (SchemaTableName tableName : listTables(session, prefix.getSchema())) {
//            Optional<ConnectorTableMetadata> tableMetadata = getTableMetadata(tableName);
//            // table can disappear during listing operation
//            if (tableMetadata.isPresent()) {
//                columns.put(tableName, tableMetadata.get().getColumns());
//            }
//        }
//        return columns.buildOrThrow();
//    }

    private Optional<ConnectorTableMetadata> getTableMetadata(SchemaTableName tableName)
    {
        if (!listSchemaNames().contains(tableName.getSchemaName())) {
            return Optional.empty();
        }
        Optional<SheetsTable> table = sheetsClient.getTable(tableName.getTableName());
        if (table.isPresent()) {
            return Optional.of(new ConnectorTableMetadata(tableName, table.get().getColumnsMetadata()));
        }
        return Optional.empty();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        String schema = schemaName.orElseGet(() -> getOnlyElement(SCHEMAS));

        if (listSchemaNames().contains(schema)) {
            return sheetsClient.getTableNames().stream()
                    .map(tableName -> new SchemaTableName(schema, tableName))
                    .collect(toImmutableList());
        }
        return ImmutableList.of();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((SheetsColumnHandle) columnHandle).getColumnMetadata();
    }
}
