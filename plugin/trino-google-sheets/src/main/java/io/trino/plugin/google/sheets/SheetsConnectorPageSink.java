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

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class SheetsConnectorPageSink
        implements ConnectorPageSink
{
    private static final Logger log = Logger.get(SheetsConnectorPageSink.class);

    private final SheetsInsertTableHandle sheetsInsertTableHandle;

    private final SheetsClient sheetsClient;

    public SheetsConnectorPageSink(SheetsInsertTableHandle sheetsInsertTableHandle, SheetsClient sheetsClient)
    {
        this.sheetsInsertTableHandle = sheetsInsertTableHandle;
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
        // default seems to be io.trino.spi.block.RunLengthEncodedBlock
        //        Block block = page.getBlock(0);
        //        log.info("block class name is %s", block.getClass().getName());
        //        log.info("block: %s", block);

        //   generate the data to write to spreadsheet
        // step1: read existing data from gsheet
        final String sheetID = getSheetId();
        final String range = getSheetRange();
        List<List<Object>> data = sheetsClient.readData(sheetID, range);

        List<SheetsColumnHandle> columnHandles = sheetsInsertTableHandle.getColumnHandles();
        Map<Integer, SheetsColumnHandle> columnsMap = columnHandles.stream().collect(Collectors.toUnmodifiableMap(SheetsColumnHandle::getOrdinalPosition, Function.identity()));

        // step 2: append data to the end of existing data
        // Note: Currently all columns are passed here (regardless of whether it is getting inserted)
        // omitted columns are null
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
        log.info("updated %d rows", sheetsClient.writeData(sheetID, range, data));
        return NOT_BLOCKED;
    }

    private String getSheetId()
    {
        return sheetsInsertTableHandle.getTableHandle().getLocation().split("#")[0];
    }

    private String getSheetRange()
    {
        String[] xs = sheetsInsertTableHandle.getTableHandle().getLocation().split("#");
        return xs[1];
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
