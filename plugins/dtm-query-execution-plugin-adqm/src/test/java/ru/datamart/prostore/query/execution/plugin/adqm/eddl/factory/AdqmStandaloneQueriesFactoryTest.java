/*
 * Copyright © 2022 DATAMART LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.datamart.prostore.query.execution.plugin.adqm.eddl.factory;

import lombok.val;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.execution.plugin.adqm.ddl.configuration.properties.DdlProperties;
import ru.datamart.prostore.query.execution.plugin.adqm.ddl.factory.ShardingExpr;
import ru.datamart.prostore.query.execution.plugin.adqm.utils.StandaloneTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static ru.datamart.prostore.query.execution.plugin.adqm.utils.StandaloneTestUtils.CLUSTER;

class AdqmStandaloneQueriesFactoryTest {
    private static Entity testEntity;
    private static final DdlProperties ddlProperties = mock(DdlProperties.class);
    private final AdqmStandaloneQueriesFactory factory = new AdqmStandaloneQueriesFactory(ddlProperties);

    @BeforeAll
    static void setUp() {
        when(ddlProperties.getCluster()).thenReturn(CLUSTER);
        when(ddlProperties.getShardingKeyExpr()).thenReturn(ShardingExpr.CITY_HASH_64);

        testEntity = StandaloneTestUtils.createTestEntity();
    }

    @Test
    void shouldCreateShardQuery() {
        val expectedShardQuery = "CREATE TABLE dbName.table_path_shard ON CLUSTER cluster\n" +
                "(ID Int64, DESCRIPTION String, FOREIGN_KEY Int64, TIME_COL Nullable(Int64))\n" +
                "ENGINE = MergeTree()\n" +
                "ORDER BY (ID)";
        assertEquals(expectedShardQuery, factory.createShardQuery(testEntity));
    }

    @Test
    void shouldCreateDistributedQuery() {
        val expectedDistributedQuery = "CREATE TABLE dbName.table_path ON CLUSTER cluster AS dbName.table_path_shard \n" +
                "ENGINE = Distributed(cluster, dbName, table_path_shard, cityHash64(ID))";
        assertEquals(expectedDistributedQuery, factory.createDistributedQuery(testEntity));
    }

}