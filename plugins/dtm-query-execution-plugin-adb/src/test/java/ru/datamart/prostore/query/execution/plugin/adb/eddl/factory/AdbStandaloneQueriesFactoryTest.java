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
package ru.datamart.prostore.query.execution.plugin.adb.eddl.factory;

import lombok.val;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static utils.CreateEntityUtils.LOCATION_PATH;
import static utils.CreateEntityUtils.getEntity;

class AdbStandaloneQueriesFactoryTest {

    @Test
    void testCreateQuery() {
        val expectedQuery = "CREATE TABLE table_path \n" +
                "(id int8 NOT NULL, sk_key2 int8 NOT NULL, pk2 int8 NOT NULL, " +
                "sk_key3 int8 NOT NULL, VARCHAR_type varchar(20) , CHAR_type varchar(20) ," +
                " BIGINT_type int8 , INT_type int8 , INT32_type int4 , " +
                "DOUBLE_type float8 , FLOAT_type float4 , DATE_type date , TIME_type time(6) , " +
                "TIMESTAMP_type timestamp(6) , BOOLEAN_type bool , UUID_type varchar(36) , " +
                "LINK_type varchar ,\n" +
                " PRIMARY KEY (id, pk2)) \n" +
                " DISTRIBUTED BY (id, sk_key2, sk_key3)";
        assertEquals(expectedQuery, AdbStandaloneQueriesFactory.createQuery(getEntity()));
    }

    @Test
    void testDropQuery() {
        val expectedQuery = "DROP TABLE table_path";
        assertEquals(expectedQuery, AdbStandaloneQueriesFactory.dropQuery(LOCATION_PATH));
    }
}