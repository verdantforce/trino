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

import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorPageSink;

public class SheetsConnectorPageSinkFactory
{
    private final SheetsClient sheetsClient;

    @Inject
    public SheetsConnectorPageSinkFactory(SheetsClient sheetsClient)
    {
        this.sheetsClient = sheetsClient;
    }

    public ConnectorPageSink create(SheetsInsertTableHandle sheetsInsertTableHandle)
    {
        return new SheetsInsertConnectorPageSink(sheetsInsertTableHandle, sheetsClient);
    }

    public ConnectorPageSink create(SheetsOutputTableHandle sheetsOutputTableHandle)
    {
        return new SheetsOutputConnectorPageSink(sheetsOutputTableHandle, sheetsClient);
    }
}
