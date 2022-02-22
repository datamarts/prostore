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
package ru.datamart.prostore.jdbc.ext;

import ru.datamart.prostore.jdbc.core.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DtmConnectionImplTest {

    private BaseConnection connection;
    private final QueryExecutor queryExecutor = mock(QueryExecutorImpl.class);
    private final ConnectionFactory connectionFactory = mock(ConnectionFactoryImpl.class);

    @BeforeEach
    void setUp() throws SQLException {
        String host = "localhost";
        String user = "dtm";
        String schema = "test";
        String url = String.format("jdbc:prostore://%s/", host);
        when(connectionFactory.openConnectionImpl(any(), any(), any(), any(), any()))
                .thenReturn(queryExecutor);
        connection = new DtmConnectionImpl(host, user, schema, null, url);
    }

    @Test
    void unwrap() throws SQLException {
        assertEquals(connection, connection.unwrap(DtmConnectionImpl.class));
        assertThrows(SQLException.class, () -> connection.unwrap(DtmConnectionImplTest.class));
    }

    @Test
    void isWrapperFor() throws SQLException {
        assertTrue(connection.isWrapperFor(DtmConnectionImpl.class));
        assertFalse(connection.isWrapperFor(null));
    }
}