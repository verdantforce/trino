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
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class SheetsOutputConnectorPageSink
        implements ConnectorPageSink
{
    private static final Logger log = Logger.get(SheetsOutputConnectorPageSink.class);

    private final SheetsOutputTableHandle sheetsOutputTableHandle;

    private final SheetsClient sheetsClient;

    public SheetsOutputConnectorPageSink(SheetsOutputTableHandle sheetsOutputTableHandle, SheetsClient sheetsClient)
    {
        this.sheetsOutputTableHandle = sheetsOutputTableHandle;
        this.sheetsClient = sheetsClient;
    }

    /**
     * Returns a future that will be completed when the page sink can accept
     * more pages.  If the page sink can accept more pages immediately,
     * this method should return {@code NOT_BLOCKED}.
     *
     * @param page
     */
    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        // differs from insert in that the old data does not need to be read
        List<SheetsColumnHandle> columnHandles = sheetsOutputTableHandle.getColumnHandles();
        Map<Integer, SheetsColumnHandle> columnsMap = columnHandles.stream().collect(Collectors.toUnmodifiableMap(SheetsColumnHandle::getOrdinalPosition, Function.identity()));

        String sheetID = getSheetId();
        String sheetRange = getSheetRange();
        List<List<Object>> data = new ArrayList<>();

        // step 1: add column headers
        List<Object> columnNames = IntStream.range(0, columnsMap.size()).mapToObj(
                i -> (Object) columnsMap.get(i).getColumnName()).toList();
        data.add(columnNames);

        // step 2: add all the data
        for (int position = 0; position < page.getPositionCount(); position++) {
            ArrayList<Object> row = new ArrayList<>();
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                // short-circuiting this since all the columns are VARCHARs
                String columnName = requireNonNull(columnsMap.get(channel)).getColumnName();
                Block block = page.getBlock(channel);
                boolean isNull = block.isNull(position);
                String cell = "";
                if (!isNull) {
                    cell = requireNonNull(columnsMap.get(channel)).getColumnType().getSlice(block, position).toStringUtf8();
                }
                row.add(cell);
                log.info("inserting row=%d, column_idx=%d, column=%s, data=%s, is_null=%b", position, channel, columnName, cell, isNull);
            }
            data.add(row);
        }

        // write to google spreadsheet
        log.info("inserted %d rows into new table `%s`", sheetsClient.writeData(sheetID, sheetRange, data), sheetsOutputTableHandle.getTableHandle().getTableName());
        return NOT_BLOCKED;
    }

    private String getSheetId()
    {
        String location = sheetsOutputTableHandle.getTableHandle().getLocation();
        return location.split("#")[0];
    }

    private String getSheetRange()
    {
        String location = sheetsOutputTableHandle.getTableHandle().getLocation();
        return location.split("#")[1];
    }

    /**
     * Notifies the connector that no more pages will be appended and returns
     * connector-specific information that will be sent to the coordinator to
     * complete the write operation. This method may be called immediately
     * after the previous call to {@link #appendPage} (even if the returned
     * future is not complete).
     */
    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
    }
}
