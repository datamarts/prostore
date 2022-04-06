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
package ru.datamart.prostore.query.execution.plugin.adp.eddl.factory;

import lombok.val;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static ru.datamart.prostore.query.execution.plugin.adp.util.TestUtils.LOCATION_PATH;
import static ru.datamart.prostore.query.execution.plugin.adp.util.TestUtils.createAllTypesTable;

class AdpStandaloneQueriesFactoryTest {

    @Test
    void testCreateQuery() {
        val expectedCreateStandaloneQuery = "CREATE TABLE table_path " +
                "(id int8 NOT NULL, varchar_col varchar(10) , char_col varchar(10) , " +
                "bigint_col int8 , int_col int8 , int32_col int4 , double_col float8 , " +
                "float_col float4 , date_col date , time_col time(6) , timestamp_col timestamp(6) , " +
                "boolean_col bool , uuid_col varchar(36) , link_col varchar , " +
                "PRIMARY KEY (id))";
        assertEquals(expectedCreateStandaloneQuery, AdpStandaloneQueriesFactory.createQuery(createAllTypesTable()));
    }

    @Test
    void testDropQuery() {
        val expectedDropStandaloneQuery = "DROP TABLE table_path";
        assertEquals(expectedDropStandaloneQuery, AdpStandaloneQueriesFactory.dropQuery(LOCATION_PATH));
    }
}