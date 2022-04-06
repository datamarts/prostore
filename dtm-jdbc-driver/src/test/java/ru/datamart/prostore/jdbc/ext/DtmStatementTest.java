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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.jdbc.core.*;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
class DtmStatementTest {

    @Mock
    private BaseConnection connection;
    @Mock
    private QueryExecutor queryExecutor;
    @Captor
    private ArgumentCaptor<List<QueryParameters>> parametersListCaptor;
    @Captor
    private ArgumentCaptor<List<Query>> queryListCaptor;

    private Statement statement;

    @BeforeEach
    void setUp() throws SQLException {
        int rsType = 0;
        int rsConcurrency = 0;
        statement = new DtmStatement(connection, rsType, rsConcurrency);
        lenient().when(connection.getQueryExecutor()).thenReturn(queryExecutor);
        lenient().when(queryExecutor.createQuery(anyString())).thenAnswer(invocationOnMock -> SqlParser.parseSql(invocationOnMock.getArgument(0)));
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

    @Test
    void shouldSuccessWhenExecuteBatch() throws SQLException {
        // arrange
        doAnswer(invocationOnMock -> {
            List<Query> queries = invocationOnMock.getArgument(0);
            ResultHandler resultHandler = invocationOnMock.getArgument(2);
            for (Query query : queries) {
                Field field = new Field("id", ColumnType.BIGINT);
                resultHandler.handleResultRows(query, new Field[]{field}, new ArrayList<>());
            }
            return null;
        }).when(queryExecutor).execute(queryListCaptor.capture(), parametersListCaptor.capture(), any());

        String sql1 = "SELECT * FROM tbl";
        statement.addBatch(sql1);
        String sql2 = "SELECT * FROM tbl2";
        statement.addBatch(sql2);
        String sql3 = "SELECT * FROM tbl3";
        statement.addBatch(sql3);

        // act
        int[] ints = statement.executeBatch();

        // assert
        assertEquals(3, ints.length);
        assertNull(parametersListCaptor.getValue());
        assertThat(queryListCaptor.getValue(), contains(
                hasProperty("nativeSql", is(sql1)),
                hasProperty("nativeSql", is(sql2)),
                hasProperty("nativeSql", is(sql3))
        ));
    }
}