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

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.UpdateValuesResponse;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class SheetsConnectorPageSink
        implements ConnectorPageSink
{
    private static final Logger log = Logger.get(SheetsConnectorPageSink.class);

    private final SheetsInsertTableHandle sheetsInsertTableHandle;
    private final String location = "1a2auf5l_uGkMPs6m2F-Q4nBpfur1qpHQ6kF7qZ66Nw8#test_table1";

    public SheetsConnectorPageSink(SheetsInsertTableHandle sheetsInsertTableHandle)
    {
        this.sheetsInsertTableHandle = sheetsInsertTableHandle;
    }

    private List<List<Object>> readData(String spreadsheetId, String range)
    {
        try {
            final NetHttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
            final JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();

            GoogleCredentials googleCredentials = GoogleCredentials.getApplicationDefault()
                    .createScoped(Collections.singletonList(SheetsScopes.SPREADSHEETS));
            Sheets service = new Sheets.Builder(httpTransport, jsonFactory, new HttpCredentialsAdapter(googleCredentials)).setApplicationName("presto-gsheet-connector").build();
            ValueRange readResponse = service.spreadsheets().values().get(spreadsheetId, range).execute();
            List<List<Object>> values = readResponse.getValues();
            if (values == null || values.isEmpty()) {
                throw new RuntimeException("no data was read!");
            }
            return values;
        }
        catch (Exception e) {
            log.error(e, "failed to read data from sheetID=%s, range=%s", spreadsheetId, range);
            throw new RuntimeException("failed to read data");
        }
    }

    private int writeData(String spreadsheetId, String range, List<List<Object>> values)
    {
        try {
            final NetHttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
            final JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();

            GoogleCredentials googleCredentials = GoogleCredentials.getApplicationDefault()
                    .createScoped(Collections.singletonList(SheetsScopes.SPREADSHEETS));
            Sheets service = new Sheets.Builder(httpTransport, jsonFactory, new HttpCredentialsAdapter(googleCredentials)).setApplicationName("presto-gsheet-connector").build();
            ValueRange writeData = new ValueRange().setValues(values);
            UpdateValuesResponse writeResponse = service.spreadsheets().values().update(spreadsheetId, range, writeData).setValueInputOption("RAW").execute();
            if (writeResponse.getUpdatedCells() == 0) {
                throw new RuntimeException("no rows were updated!");
            }
            return writeResponse.getUpdatedRows();
        }
        catch (Exception e) {
            log.error(e, "failed to write data to sheetID=%s, range=%s", spreadsheetId, range);
            throw new RuntimeException("failed to write data to gsheet!");
        }
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
        // TODO: figure out why location is not propagated to table handle
        // hardcoding gsheet id and range for now
        // note with location, a bit of string manipulation is needed to get extract the sheetID
        // and sheet name from it example
        //
        final String sheetID = getSheetId();
        final Optional<String> sheetName = getSheetName();
        // default to 10K columns for now
        final String range = sheetName.map(x -> x + "!").orElse("") + "$1:$10000";
        List<List<Object>> data = readData(sheetID, range);

        List<SheetsColumnHandle> columnHandles = sheetsInsertTableHandle.getColumnHandles();

        ImmutableMap.Builder<Integer, SheetsColumnHandle> columnsMapBuilder = ImmutableMap.builder();
        columnHandles.forEach(x -> columnsMapBuilder.put(x.getOrdinalPosition(), x));
        ImmutableMap<Integer, SheetsColumnHandle> columnsMap = columnsMapBuilder.buildOrThrow();

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
        log.info("updated %d cells", writeData(sheetID, range, data));
        return NOT_BLOCKED;
    }

    private String getSheetId()
    {
        return location.split("#")[0];
    }

    private Optional<String> getSheetName()
    {
        String[] xs = location.split("#");
        if (xs.length > 1) {
            // there is a sheet/tab name within the sheet
            return Optional.of(xs[1]);
        }
        else {
            return Optional.empty();
        }
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
