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
import com.google.api.services.sheets.v4.model.AddSheetRequest;
import com.google.api.services.sheets.v4.model.AppendValuesResponse;
import com.google.api.services.sheets.v4.model.BatchUpdateSpreadsheetRequest;
import com.google.api.services.sheets.v4.model.Request;
import com.google.api.services.sheets.v4.model.SheetProperties;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.UpdateValuesResponse;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.collect.cache.NonEvictableLoadingCache;
import io.trino.spi.TrinoException;
import io.trino.spi.type.VarcharType;

import javax.inject.Inject;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.plugin.google.sheets.SheetsErrorCode.SHEETS_METASTORE_ERROR;
import static io.trino.plugin.google.sheets.SheetsErrorCode.SHEETS_TABLE_LOAD_ERROR;
import static io.trino.plugin.google.sheets.SheetsErrorCode.SHEETS_UNKNOWN_TABLE_ERROR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SheetsClient
{
    private static final Logger log = Logger.get(SheetsClient.class);

    public static final String APPLICATION_NAME = "trino google sheets integration";
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

    private static final List<String> READ_SCOPES = Collections.singletonList(SheetsScopes.SPREADSHEETS_READONLY);
    private static final List<String> WRITE_SCOPES = Collections.singletonList(SheetsScopes.SPREADSHEETS);

    private final NonEvictableLoadingCache<String, Optional<String>> tableSheetMappingCache;
    private final NonEvictableLoadingCache<String, List<List<Object>>> sheetDataCache;

    private final String metadataSheetId;

    private final Supplier<Sheets> readSheetsService = Suppliers.memoize(() -> createSheetsService(true));
    private final Supplier<Sheets> writeSheetsService = Suppliers.memoize(() -> createSheetsService(false));

    @Inject
    public SheetsClient(SheetsConfig config, JsonCodec<Map<String, List<SheetsTable>>> catalogCodec)
    {
        requireNonNull(catalogCodec, "catalogCodec is null");

        this.metadataSheetId = config.getMetadataSheetId();

        long expiresAfterWriteMillis = config.getSheetsDataExpireAfterWrite().toMillis();
        long maxCacheSize = config.getSheetsDataMaxCacheSize();

        this.tableSheetMappingCache = buildNonEvictableCache(
                newCacheBuilder(expiresAfterWriteMillis, maxCacheSize),
                new CacheLoader<>()
                {
                    @Override
                    public Optional<String> load(String tableName)
                    {
                        return getSheetExpressionForTable(tableName);
                    }

                    @Override
                    public Map<String, Optional<String>> loadAll(Iterable<? extends String> tableList)
                    {
                        return getAllTableSheetExpressionMapping();
                    }
                });

        this.sheetDataCache = buildNonEvictableCache(
                newCacheBuilder(expiresAfterWriteMillis, maxCacheSize),
                CacheLoader.from(this::readAllValuesFromSheetExpression));
    }

    public Optional<SheetsTable> getTable(String tableName)
    {
        Optional<String> location = tableSheetMappingCache.getUnchecked(tableName);
        if (location.isEmpty()) {
            return Optional.empty();
        }

        List<List<String>> values = convertToStringValues(readAllValues(tableName));
        if (values.size() > 0) {
            ImmutableList.Builder<SheetsColumn> columns = ImmutableList.builder();
            Set<String> columnNames = new HashSet<>();
            // Assuming 1st line is always header
            List<String> header = values.get(0);
            int count = 0;
            for (String column : header) {
                String columnValue = column.toLowerCase(ENGLISH);
                // when empty or repeated column header, adding a placeholder column name
                if (columnValue.isEmpty() || columnNames.contains(columnValue)) {
                    columnValue = "column_" + ++count;
                }
                columnNames.add(columnValue);
                columns.add(new SheetsColumn(columnValue, VarcharType.VARCHAR));
            }
            List<List<String>> dataValues = values.subList(1, values.size()); // removing header info
            SheetsTable sheetsTable = new SheetsTable(tableName, location.get(), columns.build(), dataValues);
            //            log.info("getTable %s: %s", tableName, sheetsTable);
            return Optional.of(sheetsTable);
        }
        return Optional.empty();
    }

    public Set<String> getTableNames()
    {
        ImmutableSet.Builder<String> tables = ImmutableSet.builder();
        try {
            List<List<Object>> tableMetadata = sheetDataCache.getUnchecked(metadataSheetId);
            for (int i = 1; i < tableMetadata.size(); i++) {
                if (tableMetadata.get(i).size() > 0) {
                    tables.add(String.valueOf(tableMetadata.get(i).get(0)));
                }
            }
            return tables.build();
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new TrinoException(SHEETS_METASTORE_ERROR, e);
        }
    }

    public List<String> getSheetTabs(String sheetId)
    {
        try {
            Spreadsheet spreadsheet = readSheetsService.get().spreadsheets().get(sheetId).execute();
            return spreadsheet.getSheets().stream().map(sheet -> sheet.getProperties().getTitle()).toList();
        }
        catch (Exception e) {
            log.error(e, "failed to read data from sheetID %s", sheetId);
            throw new RuntimeException("failed to read all sheet tabs");
        }
    }

    public void createSheetTab(String sheetId, String sheetTab)
    {
        try {
            Request request = new Request().setAddSheet(
                    new AddSheetRequest().setProperties(
                            new SheetProperties().setTitle(sheetTab)));
            BatchUpdateSpreadsheetRequest batchUpdateSpreadsheetRequest = new BatchUpdateSpreadsheetRequest().setRequests(
                    ImmutableList.of(request));
            writeSheetsService.get().spreadsheets().batchUpdate(sheetId, batchUpdateSpreadsheetRequest).execute();
        }
        catch (Exception e) {
            log.error(e, "failed to read data from sheetID %s", sheetId);
            throw new RuntimeException("failed to read all sheet tabs");
        }
    }

    public int insertToSheet(String sheetID, String sheetTab, List<List<Object>> data)
    {
        try {
            ValueRange appendData = new ValueRange().setValues(data);
            AppendValuesResponse response = writeSheetsService.get().spreadsheets().values().append(sheetID, sheetTab + "!A1", appendData).setValueInputOption("RAW").setInsertDataOption("INSERT_ROWS").execute();
            return response.getUpdates().getUpdatedRows();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void insertTableMapping(String tableName, String location)
    {
        String[] tableOptions = metadataSheetId.split("#");
        String sheetId = tableOptions[0];
        String sheetName = tableOptions[1];
        List<List<Object>> data = ImmutableList.of(
                ImmutableList.of(
                        tableName, // tableName
                        location, // location
                        APPLICATION_NAME, // owner
                        "")); // notes
        // update table metadata spreadsheet with new table
        insertToSheet(sheetId, sheetName, data);

        // update the table metadata directly
        tableSheetMappingCache.put(
                tableName,
                Optional.of(location));
    }

    public List<List<Object>> readAllValues(String tableName)
    {
        try {
            String sheetExpression = tableSheetMappingCache.getUnchecked(tableName)
                    .orElseThrow(() -> new TrinoException(SHEETS_UNKNOWN_TABLE_ERROR, "Sheet expression not found for table " + tableName));
            return sheetDataCache.getUnchecked(sheetExpression);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new TrinoException(SHEETS_TABLE_LOAD_ERROR, "Error loading data for table: " + tableName, e);
        }
    }

    public static List<List<String>> convertToStringValues(List<List<Object>> values)
    {
        return values.stream()
                .map(columns -> columns.stream().map(String::valueOf).collect(toImmutableList()))
                .collect(toImmutableList());
    }

    private Optional<String> getSheetExpressionForTable(String tableName)
    {
        Map<String, Optional<String>> tableSheetMap = getAllTableSheetExpressionMapping();
        if (!tableSheetMap.containsKey(tableName)) {
            return Optional.empty();
        }
        return tableSheetMap.get(tableName);
    }

    private Map<String, Optional<String>> getAllTableSheetExpressionMapping()
    {
        ImmutableMap.Builder<String, Optional<String>> tableSheetMap = ImmutableMap.builder();
        List<List<Object>> data = readAllValuesFromSheetExpression(metadataSheetId);
        // first line is assumed to be sheet header
        for (int i = 1; i < data.size(); i++) {
            if (data.get(i).size() >= 2) {
                String tableId = String.valueOf(data.get(i).get(0));
                String sheetId = String.valueOf(data.get(i).get(1));
                tableSheetMap.put(tableId.toLowerCase(Locale.ENGLISH), Optional.of(sheetId));
            }
        }
        return tableSheetMap.buildOrThrow();
    }

    private List<List<Object>> readAllValuesFromSheetExpression(String sheetExpression)
    {
        String[] tableOptions = sheetExpression.split("#");
        String sheetId = tableOptions[0];
        String defaultRange = "";
        if (tableOptions.length > 1) {
            defaultRange = tableOptions[1];
        }
        log.info("Accessing sheet id [%s] with range [%s]", sheetId, defaultRange);
        return readData(sheetId, defaultRange);
    }

    private Sheets createSheetsService(Boolean readOnly)
    {
        List<String> scopes = readOnly ? READ_SCOPES : WRITE_SCOPES;
        try {
            NetHttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
            GoogleCredentials googleCredentials = GoogleCredentials.getApplicationDefault().createScoped(scopes);
            return new Sheets.Builder(httpTransport, JSON_FACTORY, new HttpCredentialsAdapter(googleCredentials)).setApplicationName(APPLICATION_NAME).build();
        }
        catch (Exception e) {
            log.error(e, "failed to create SheetsService (readOnly %b)", readOnly);
            throw new RuntimeException("failed to create SheetsService");
        }
    }

    public List<List<Object>> readData(String spreadsheetId, String range)
    {
        try {
            List<List<Object>> values = readSheetsService.get().spreadsheets().values().get(spreadsheetId, range).execute().getValues();
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

    public int writeData(String spreadsheetId, String range, List<List<Object>> values)
    {
        try {
            ValueRange writeData = new ValueRange().setValues(values);
            UpdateValuesResponse writeResponse = writeSheetsService.get().spreadsheets().values().update(spreadsheetId, range, writeData).setValueInputOption("RAW").execute();
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

    private static CacheBuilder<Object, Object> newCacheBuilder(long expiresAfterWriteMillis, long maximumSize)
    {
        return CacheBuilder.newBuilder().expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS).maximumSize(maximumSize);
    }
}
