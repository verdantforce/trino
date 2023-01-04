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
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnMetadata;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class SheetsTable
{
    private final List<ColumnMetadata> columnsMetadata;
    private final String location;
    private final List<List<String>> values;
    private final String name;

    @JsonCreator
    public SheetsTable(
            @JsonProperty("name") String name,
            @JsonProperty("location") String location,
            @JsonProperty("columns") List<SheetsColumn> columns,
            @JsonProperty("values") List<List<String>> values)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        requireNonNull(columns, "columns is null");

        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        for (SheetsColumn column : columns) {
            columnsMetadata.add(new ColumnMetadata(column.getName(), column.getType()));
        }
        this.columnsMetadata = columnsMetadata.build();
        this.values = values;
        this.location = location;
        this.name = name;
    }

    @JsonProperty
    public List<List<String>> getValues()
    {
        return values;
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        return columnsMetadata;
    }

    @JsonProperty
    public String getLocation()
    {
        return location;
    }

    @Override
    public String toString()
    {
        List<String> columns = columnsMetadata.stream().map(ColumnMetadata::getName).toList();
        String columnInfo = "[" + String.join(",", columns) + "]";
        return toStringHelper(this)
                .add("name", name)
                .add("location", location)
                .add("columns", columnInfo)
                .toString();
    }
}
