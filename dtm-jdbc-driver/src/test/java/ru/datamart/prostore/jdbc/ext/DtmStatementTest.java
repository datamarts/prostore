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

import ru.datamart.prostore.jdbc.core.BaseConnection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class DtmStatementTest {

    private final BaseConnection connection = mock(DtmConnectionImpl.class);
    private Statement statement;

    @BeforeEach
    void setUp() {
        int rsType = 0;
        int rsConcurrency = 0;
        statement = new DtmStatement(connection, rsType, rsConcurrency);
    }

    @Test
    void unwrap() throws SQLException {
        assertEquals(statement, statement.unwrap(DtmStatement.class));
        assertThrows(SQLException.class, () -> statement.unwrap(DtmStatementTest.class));
    }

    @Test
    void isWrapperFor() throws SQLException {
        assertTrue(statement.isWrapperFor(DtmStatement.class));
        assertFalse(statement.isWrapperFor(null));
    }
}