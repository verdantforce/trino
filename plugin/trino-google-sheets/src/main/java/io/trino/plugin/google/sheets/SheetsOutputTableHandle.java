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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorOutputTableHandle;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

public class SheetsOutputTableHandle
        implements ConnectorOutputTableHandle
{
    private final SheetsTableHandle tableHandle;
    private final List<SheetsColumnHandle> columnHandles;

    @JsonCreator
    public SheetsOutputTableHandle(
            @JsonProperty("tableHandle") SheetsTableHandle tableHandle,
            @JsonProperty("columnHandles") List<SheetsColumnHandle> columnHandles)
    {
        this.tableHandle = tableHandle;
        this.columnHandles = columnHandles;
    }

    @JsonProperty
    public SheetsTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public List<SheetsColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }

    @Override
    public String toString()
    {
        List<String> columns = columnHandles.stream().map(SheetsColumnHandle::toString).toList();
        String columnInfo = "[" + String.join(",", columns) + "]";
        return toStringHelper(this)
                .add("schemaName", tableHandle.getSchemaName())
                .add("tableName", tableHandle.getTableName())
                .add("location", tableHandle.getLocation())
                .add("columns", columnInfo)
                .toString();
    }
}
