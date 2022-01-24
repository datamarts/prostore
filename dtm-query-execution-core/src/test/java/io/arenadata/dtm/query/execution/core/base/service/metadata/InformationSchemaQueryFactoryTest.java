/*
 * Copyright © 2021 ProStore
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
package io.arenadata.dtm.query.execution.core.base.service.metadata;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class InformationSchemaQueryFactoryTest {

    private DataTypeMapper dataTypeMapper;
    private InformationSchemaQueryFactory informationSchemaQueryFactory;

    @Before
    public void setUp() {
        dataTypeMapper = new DataTypeMapper();
        informationSchemaQueryFactory = new InformationSchemaQueryFactory(dataTypeMapper);
    }

    @Test
    public void testDataTypeMapping() {
        String mappedTypes = informationSchemaQueryFactory.createInitEntitiesQuery().toUpperCase();

        assertTrue(mappedTypes.contains("WHEN DATA_TYPE = 'CHARACTER' THEN 'CHAR'"));
        assertTrue(mappedTypes.contains("WHEN DATA_TYPE = 'CHARACTER VARYING' THEN 'VARCHAR'"));
        assertTrue(mappedTypes.contains("WHEN DATA_TYPE = 'LONGVARCHAR' THEN 'VARCHAR'"));
        assertTrue(mappedTypes.contains("WHEN DATA_TYPE = 'SMALLINT' THEN 'INT32'"));
        assertTrue(mappedTypes.contains("WHEN DATA_TYPE = 'INTEGER' THEN 'INT'"));
        assertTrue(mappedTypes.contains("WHEN DATA_TYPE = 'TINYINT' THEN 'INT32'"));
        assertTrue(mappedTypes.contains("WHEN DATA_TYPE = 'DOUBLE PRECISION' THEN 'DOUBLE'"));
        assertTrue(mappedTypes.contains("WHEN DATA_TYPE = 'DECIMAL' THEN 'DOUBLE'"));
        assertTrue(mappedTypes.contains("WHEN DATA_TYPE = 'DEC' THEN 'DOUBLE'"));
        assertTrue(mappedTypes.contains("WHEN DATA_TYPE = 'NUMERIC' THEN 'DOUBLE'"));
    }

}
