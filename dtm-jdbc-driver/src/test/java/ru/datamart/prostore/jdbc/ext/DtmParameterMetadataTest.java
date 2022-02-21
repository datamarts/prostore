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
package ru.datamart.prostore.jdbc.ext;

import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.jdbc.core.BaseConnection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class DtmParameterMetadataTest {

    private final BaseConnection connection = mock(DtmConnectionImpl.class);
    private ParameterMetaData parameterMetaData;

    @BeforeEach
    void setUp() {
        ColumnType[] paramTypes = new ColumnType[0];
        parameterMetaData = new DtmParameterMetadata(connection, paramTypes);
    }

    @Test
    void unwrap() throws SQLException {
        assertEquals(parameterMetaData, parameterMetaData.unwrap(DtmParameterMetadata.class));
        assertThrows(SQLException.class, () -> parameterMetaData.unwrap(DtmParameterMetadataTest.class));
    }

    @Test
    void isWrapperFor() throws SQLException {
        assertTrue(parameterMetaData.isWrapperFor(DtmParameterMetadata.class));
        assertFalse(parameterMetaData.isWrapperFor(null));
    }
}