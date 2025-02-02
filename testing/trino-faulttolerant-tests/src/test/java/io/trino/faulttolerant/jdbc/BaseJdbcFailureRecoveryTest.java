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
package io.trino.faulttolerant.jdbc;

import io.trino.faulttolerant.BaseFailureRecoveryTest;
import io.trino.operator.RetryPolicy;
import org.testng.SkipException;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseJdbcFailureRecoveryTest
        extends BaseFailureRecoveryTest
{
    public BaseJdbcFailureRecoveryTest(RetryPolicy retryPolicy)
    {
        super(retryPolicy);
    }

    @Override
    protected void createPartitionedLineitemTable(String tableName, List<String> columns, String partitionColumn)
    {
    }

    @Override
    public void testJoinDynamicFilteringDisabled()
    {
        assertThatThrownBy(super::testJoinDynamicFilteringDisabled)
                .hasMessageContaining("partitioned_lineitem' does not exist");
        throw new SkipException("skipped");
    }

    @Override
    public void testJoinDynamicFilteringEnabled()
    {
        assertThatThrownBy(super::testJoinDynamicFilteringEnabled)
                .hasMessageContaining("partitioned_lineitem' does not exist");
        throw new SkipException("skipped");
    }

    @Override
    public void testAnalyzeTable()
    {
        assertThatThrownBy(super::testAnalyzeTable).hasMessageMatching("This connector does not support analyze");
        throw new SkipException("skipped");
    }

    @Override
    public void testDelete()
    {
        // This simple delete on JDBC ends up as a very simple, single-fragment, coordinator-only plan,
        // which has no ability to recover from errors. This test simply verifies that's still the case.
        Optional<String> setupQuery = Optional.of("CREATE TABLE <table> AS SELECT * FROM orders");
        String testQuery = "DELETE FROM <table> WHERE orderkey = 1";
        Optional<String> cleanupQuery = Optional.of("DROP TABLE <table>");

        assertThatQuery(testQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .isCoordinatorOnly();
    }

    @Override
    public void testDeleteWithSubquery()
    {
        assertThatThrownBy(super::testDeleteWithSubquery).hasMessageContaining(MODIFYING_ROWS_MESSAGE);
        throw new SkipException("skipped");
    }

    @Override
    public void testRefreshMaterializedView()
    {
        assertThatThrownBy(super::testRefreshMaterializedView)
                .hasMessageContaining("This connector does not support creating materialized views");
        throw new SkipException("skipped");
    }

    @Override
    public void testUpdate()
    {
        assertThatThrownBy(super::testUpdate).hasMessageContaining(MODIFYING_ROWS_MESSAGE);
        throw new SkipException("skipped");
    }

    @Override
    public void testUpdateWithSubquery()
    {
        assertThatThrownBy(super::testUpdateWithSubquery).hasMessageContaining(MODIFYING_ROWS_MESSAGE);
        throw new SkipException("skipped");
    }

    @Override
    public void testMerge()
    {
        assertThatThrownBy(super::testMerge).hasMessageContaining(MODIFYING_ROWS_MESSAGE);
        throw new SkipException("skipped");
    }

    @Override
    protected boolean areWriteRetriesSupported()
    {
        return true;
    }
}
