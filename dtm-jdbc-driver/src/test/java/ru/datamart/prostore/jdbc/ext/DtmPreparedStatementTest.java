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

import org.apache.commons.lang3.StringUtils;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.util.DateTimeUtils;
import ru.datamart.prostore.jdbc.core.*;
import ru.datamart.prostore.jdbc.util.DtmSqlException;
import ru.datamart.prostore.jdbc.utils.TestUtils;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DtmPreparedStatementTest {

    @Mock
    private BaseConnection baseConnection;

    @Mock
    private QueryExecutor queryExecutor;

    @Captor
    private ArgumentCaptor<QueryParameters> queryParametersCaptor;

    @Captor
    private ArgumentCaptor<List<QueryParameters>> parametersListCaptor;

    @Captor
    private ArgumentCaptor<List<Query>> queryListCaptor;

    private DtmPreparedStatement dtmPreparedStatement;
    private TimeZone INITIAL_TIMEZONE;

    @BeforeEach
    void setUp() throws SQLException {
        INITIAL_TIMEZONE = TimeZone.getDefault();
        when(baseConnection.getQueryExecutor()).thenReturn(queryExecutor);
        when(queryExecutor.createQuery(Mockito.any())).thenReturn(Collections.emptyList());
        dtmPreparedStatement = new DtmPreparedStatement(baseConnection, 1, 1, "?");
    }

    @AfterEach
    void cleanUp() {
        TimeZone.setDefault(INITIAL_TIMEZONE);
    }

    @Test
    void shouldCorrectlyPutDateWhenUTC() throws SQLException {
        // arrange
        List<LocalDate> localDates = Arrays.asList(
                LocalDate.of(2020, 1, 1),
                LocalDate.of(1990, 6, 6),
                LocalDate.of(1900, 1, 1),
                LocalDate.of(1500, 5, 12),
                LocalDate.of(1000, 6, 11),
                LocalDate.of(123, 11, 20)
        );

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Calendar cal = Calendar.getInstance();

        // act
        List<Long> results = new ArrayList<>();
        for (LocalDate localDate : localDates) {
            cal.set(localDate.getYear(), localDate.getMonthValue() - 1, localDate.getDayOfMonth());
            dtmPreparedStatement.setDate(1, new Date(cal.getTimeInMillis()));
            Long value = (Long) dtmPreparedStatement.parameters.getValues()[0];
            results.add(value);
        }

        // assert
        List<LocalDate> resultLocalDates = results.stream().map(DateTimeUtils::toLocalDate)
                .collect(Collectors.toList());
        MatcherAssert.assertThat(resultLocalDates, Matchers.contains(
                is(LocalDate.of(2020, 1, 1)),
                is(LocalDate.of(1990, 6, 6)),
                is(LocalDate.of(1900, 1, 1)),
                is(LocalDate.of(1500, 5, 12)),
                is(LocalDate.of(1000, 6, 11)),
                is(LocalDate.of(123, 11, 20))
        ));
    }

    @Test
    void shouldCorrectlyPutDateWhenShiftedGMT() throws SQLException {
        // arrange
        List<LocalDate> localDates = Arrays.asList(
                LocalDate.of(2020, 1, 1),
                LocalDate.of(1990, 6, 6),
                LocalDate.of(1900, 1, 1),
                LocalDate.of(1500, 5, 12),
                LocalDate.of(1000, 6, 11),
                LocalDate.of(123, 11, 20)
        );

        TimeZone timeZone = TimeZone.getTimeZone("GMT+11:30");
        TimeZone.setDefault(timeZone);
        Calendar cal = Calendar.getInstance();

        // act
        List<Long> results = new ArrayList<>();
        for (LocalDate localDate : localDates) {
            cal.set(localDate.getYear(), localDate.getMonthValue() - 1, localDate.getDayOfMonth());
            dtmPreparedStatement.setDate(1, new Date(cal.getTimeInMillis()));
            Long value = (Long) dtmPreparedStatement.parameters.getValues()[0];
            results.add(value);
        }

        // assert
        List<LocalDate> resultLocalDates = results.stream().map(DateTimeUtils::toLocalDate)
                .collect(Collectors.toList());
        MatcherAssert.assertThat(resultLocalDates, Matchers.contains(
                is(LocalDate.of(2020, 1, 1)),
                is(LocalDate.of(1990, 6, 6)),
                is(LocalDate.of(1900, 1, 1)),
                is(LocalDate.of(1500, 5, 12)),
                is(LocalDate.of(1000, 6, 11)),
                is(LocalDate.of(123, 11, 20))
        ));
    }

    @Test
    void shouldCorrectlyPutDateWhenNotUTC() throws SQLException {
        // arrange
        List<LocalDate> localDates = Arrays.asList(
                LocalDate.of(2020, 1, 1),
                LocalDate.of(1990, 6, 6),
                LocalDate.of(1900, 1, 1),
                LocalDate.of(1500, 5, 12),
                LocalDate.of(1000, 6, 11),
                LocalDate.of(123, 11, 20)
        );

        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Novosibirsk"));
        Calendar cal = Calendar.getInstance();

        // act
        List<Long> results = new ArrayList<>();
        for (LocalDate localDate : localDates) {
            cal.set(localDate.getYear(), localDate.getMonthValue() - 1, localDate.getDayOfMonth());
            dtmPreparedStatement.setDate(1, new Date(cal.getTimeInMillis()));
            Long value = (Long) dtmPreparedStatement.parameters.getValues()[0];
            results.add(value);
        }

        // assert
        List<LocalDate> resultLocalDates = results.stream().map(DateTimeUtils::toLocalDate)
                .collect(Collectors.toList());
        MatcherAssert.assertThat(resultLocalDates, Matchers.contains(
                is(LocalDate.of(2020, 1, 1)),
                is(LocalDate.of(1990, 6, 6)),
                is(LocalDate.of(1900, 1, 1)),
                is(LocalDate.of(1500, 5, 12)),
                is(LocalDate.of(1000, 6, 11)),
                is(LocalDate.of(123, 11, 20))
        ));
    }

    @Test
    void shouldBeSameTimeWhenUTC() throws SQLException {
        // arrange
        List<LocalTime> localTimes = Arrays.asList(
                LocalTime.of(1, 1, 11, 123_000_000),
                LocalTime.of(5, 5, 11, 123_000_000),
                LocalTime.of(9, 9, 11, 123_000_000),
                LocalTime.of(12, 12, 30, 123_000_000),
                LocalTime.of(16, 16, 40, 123_000_000),
                LocalTime.of(23, 23, 50, 123_000_000)
        );

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Calendar cal = Calendar.getInstance();

        // act
        List<Long> results = new ArrayList<>();
        for (LocalTime localTime : localTimes) {
            cal.set(Calendar.HOUR_OF_DAY, localTime.getHour());
            cal.set(Calendar.MINUTE, localTime.getMinute());
            cal.set(Calendar.SECOND, localTime.getSecond());
            cal.set(Calendar.MILLISECOND, (int) (localTime.getNano() / 1000_000L));
            dtmPreparedStatement.setTime(1, new Time(cal.getTimeInMillis()));
            Long value = (Long) dtmPreparedStatement.parameters.getValues()[0];
            results.add(value);
        }

        // assert
        List<LocalTime> result = results.stream().map(DateTimeUtils::toLocalTime)
                .collect(Collectors.toList());
        assertThat(result, contains(
                is(LocalTime.of(1, 1, 11, 123_000_000)),
                is(LocalTime.of(5, 5, 11, 123_000_000)),
                is(LocalTime.of(9, 9, 11, 123_000_000)),
                is(LocalTime.of(12, 12, 30, 123_000_000)),
                is(LocalTime.of(16, 16, 40, 123_000_000)),
                is(LocalTime.of(23, 23, 50, 123_000_000))
        ));
    }

    @Test
    void shouldBeSameTimeWhenShiftedGMT() throws SQLException {
        // arrange
        List<LocalTime> localTimes = Arrays.asList(
                LocalTime.of(1, 1, 11, 123_000_000),
                LocalTime.of(5, 5, 11, 123_000_000),
                LocalTime.of(9, 9, 11, 123_000_000),
                LocalTime.of(12, 12, 30, 123_000_000),
                LocalTime.of(16, 16, 40, 123_000_000),
                LocalTime.of(23, 23, 50, 123_000_000)
        );

        TimeZone.setDefault(TimeZone.getTimeZone("GMT+11:30"));
        Calendar cal = Calendar.getInstance();

        // act
        List<Long> results = new ArrayList<>();
        for (LocalTime localTime : localTimes) {
            cal.set(Calendar.HOUR_OF_DAY, localTime.getHour());
            cal.set(Calendar.MINUTE, localTime.getMinute());
            cal.set(Calendar.SECOND, localTime.getSecond());
            cal.set(Calendar.MILLISECOND, (int) (localTime.getNano() / 1000_000L));
            dtmPreparedStatement.setTime(1, new Time(cal.getTimeInMillis()));
            Long value = (Long) dtmPreparedStatement.parameters.getValues()[0];
            results.add(value);
        }

        // assert
        List<LocalTime> result = results.stream().map(DateTimeUtils::toLocalTime)
                .collect(Collectors.toList());
        assertThat(result, contains(
                is(LocalTime.of(1, 1, 11, 123_000_000)),
                is(LocalTime.of(5, 5, 11, 123_000_000)),
                is(LocalTime.of(9, 9, 11, 123_000_000)),
                is(LocalTime.of(12, 12, 30, 123_000_000)),
                is(LocalTime.of(16, 16, 40, 123_000_000)),
                is(LocalTime.of(23, 23, 50, 123_000_000))
        ));
    }

    @Test
    void shouldBeSameTimeWhenNotUTC() throws SQLException {
        // arrange
        List<LocalTime> localTimes = Arrays.asList(
                LocalTime.of(1, 1, 11, 123_000_000),
                LocalTime.of(5, 5, 11, 123_000_000),
                LocalTime.of(9, 9, 11, 123_000_000),
                LocalTime.of(12, 12, 30, 123_000_000),
                LocalTime.of(16, 16, 40, 123_000_000),
                LocalTime.of(23, 23, 50, 123_000_000)
        );

        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Novosibirsk"));
        Calendar cal = Calendar.getInstance();

        // act
        List<Long> results = new ArrayList<>();
        for (LocalTime localTime : localTimes) {
            cal.set(Calendar.HOUR_OF_DAY, localTime.getHour());
            cal.set(Calendar.MINUTE, localTime.getMinute());
            cal.set(Calendar.SECOND, localTime.getSecond());
            cal.set(Calendar.MILLISECOND, (int) (localTime.getNano() / 1000_000L));
            dtmPreparedStatement.setTime(1, new Time(cal.getTimeInMillis()));
            Long value = (Long) dtmPreparedStatement.parameters.getValues()[0];
            results.add(value);
        }

        // assert
        List<LocalTime> result = results.stream().map(DateTimeUtils::toLocalTime)
                .collect(Collectors.toList());
        assertThat(result, contains(
                is(LocalTime.of(1, 1, 11, 123_000_000)),
                is(LocalTime.of(5, 5, 11, 123_000_000)),
                is(LocalTime.of(9, 9, 11, 123_000_000)),
                is(LocalTime.of(12, 12, 30, 123_000_000)),
                is(LocalTime.of(16, 16, 40, 123_000_000)),
                is(LocalTime.of(23, 23, 50, 123_000_000))
        ));
    }

    @Test
    void shouldBeSameTimestampWhenUTC() throws SQLException {
        // arrange
        List<LocalDateTime> localDateTimes = Arrays.asList(
                LocalDateTime.of(2020, 1, 1, 1, 1, 11, 123_456_000),
                LocalDateTime.of(1990, 6, 6, 5, 5, 11, 123_456_000),
                LocalDateTime.of(1900, 1, 1, 9, 9, 11, 123_456_000),
                LocalDateTime.of(1500, 5, 12, 12, 12, 30, 123_456_000),
                LocalDateTime.of(1000, 6, 11, 16, 16, 40, 123_456_000),
                LocalDateTime.of(123, 11, 20, 23, 23, 50, 123_456_000)
        );

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Calendar cal = Calendar.getInstance();

        // act
        List<Long> results = new ArrayList<>();
        for (LocalDateTime localDateTime : localDateTimes) {
            cal.set(Calendar.YEAR, localDateTime.getYear());
            cal.set(Calendar.MONTH, localDateTime.getMonthValue() - 1);
            cal.set(Calendar.DAY_OF_MONTH, localDateTime.getDayOfMonth());
            cal.set(Calendar.HOUR_OF_DAY, localDateTime.getHour());
            cal.set(Calendar.MINUTE, localDateTime.getMinute());
            cal.set(Calendar.SECOND, localDateTime.getSecond());
            Timestamp timestamp = new Timestamp(cal.getTimeInMillis());
            timestamp.setNanos(localDateTime.getNano());
            dtmPreparedStatement.setTimestamp(1, timestamp);
            Long value = (Long) dtmPreparedStatement.parameters.getValues()[0];
            results.add(value);
        }

        // assert
        List<LocalDateTime> result = results.stream().map(DateTimeUtils::toLocalDateTime)
                .collect(Collectors.toList());
        assertThat(result, contains(
                is(LocalDateTime.of(2020, 1, 1, 1, 1, 11, 123_456_000)),
                is(LocalDateTime.of(1990, 6, 6, 5, 5, 11, 123_456_000)),
                is(LocalDateTime.of(1900, 1, 1, 9, 9, 11, 123_456_000)),
                is(LocalDateTime.of(1500, 5, 12, 12, 12, 30, 123_456_000)),
                is(LocalDateTime.of(1000, 6, 11, 16, 16, 40, 123_456_000)),
                is(LocalDateTime.of(123, 11, 20, 23, 23, 50, 123_456_000))
        ));
    }

    @Test
    void shouldBeSameTimestampWhenShiftedGMT() throws SQLException {
        // arrange
        List<LocalDateTime> localDateTimes = Arrays.asList(
                LocalDateTime.of(2020, 1, 1, 1, 1, 11, 123_456_000),
                LocalDateTime.of(1990, 6, 6, 5, 5, 11, 123_456_000),
                LocalDateTime.of(1900, 1, 1, 9, 9, 11, 123_456_000),
                LocalDateTime.of(1500, 5, 12, 12, 12, 30, 123_456_000),
                LocalDateTime.of(1000, 6, 11, 16, 16, 40, 123_456_000),
                LocalDateTime.of(123, 11, 20, 23, 23, 50, 123_456_000)
        );

        TimeZone.setDefault(TimeZone.getTimeZone("GMT+11:30"));
        Calendar cal = Calendar.getInstance();

        // act
        List<Long> results = new ArrayList<>();
        for (LocalDateTime localDateTime : localDateTimes) {
            cal.set(Calendar.YEAR, localDateTime.getYear());
            cal.set(Calendar.MONTH, localDateTime.getMonthValue() - 1);
            cal.set(Calendar.DAY_OF_MONTH, localDateTime.getDayOfMonth());
            cal.set(Calendar.HOUR_OF_DAY, localDateTime.getHour());
            cal.set(Calendar.MINUTE, localDateTime.getMinute());
            cal.set(Calendar.SECOND, localDateTime.getSecond());
            Timestamp timestamp = new Timestamp(cal.getTimeInMillis());
            timestamp.setNanos(localDateTime.getNano());
            dtmPreparedStatement.setTimestamp(1, timestamp);
            Long value = (Long) dtmPreparedStatement.parameters.getValues()[0];
            results.add(value);
        }

        // assert
        List<LocalDateTime> result = results.stream().map(DateTimeUtils::toLocalDateTime)
                .collect(Collectors.toList());
        assertThat(result, contains(
                is(LocalDateTime.of(2020, 1, 1, 1, 1, 11, 123_456_000)),
                is(LocalDateTime.of(1990, 6, 6, 5, 5, 11, 123_456_000)),
                is(LocalDateTime.of(1900, 1, 1, 9, 9, 11, 123_456_000)),
                is(LocalDateTime.of(1500, 5, 12, 12, 12, 30, 123_456_000)),
                is(LocalDateTime.of(1000, 6, 11, 16, 16, 40, 123_456_000)),
                is(LocalDateTime.of(123, 11, 20, 23, 23, 50, 123_456_000))
        ));
    }

    @Test
    void shouldBeSameTimestampWhenNotUTC() throws SQLException {
        // arrange
        List<LocalDateTime> localDateTimes = Arrays.asList(
                LocalDateTime.of(2020, 1, 1, 1, 1, 11, 123_456_000),
                LocalDateTime.of(1990, 6, 6, 5, 5, 11, 123_456_000),
                LocalDateTime.of(1900, 1, 1, 9, 9, 11, 123_456_000),
                LocalDateTime.of(1500, 5, 12, 12, 12, 30, 123_456_000),
                LocalDateTime.of(1000, 6, 11, 16, 16, 40, 123_456_000),
                LocalDateTime.of(123, 11, 20, 23, 23, 50, 123_456_000)
        );

        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Novosibirsk"));
        Calendar cal = Calendar.getInstance();

        // act
        List<Long> results = new ArrayList<>();
        for (LocalDateTime localDateTime : localDateTimes) {
            cal.set(Calendar.YEAR, localDateTime.getYear());
            cal.set(Calendar.MONTH, localDateTime.getMonthValue() - 1);
            cal.set(Calendar.DAY_OF_MONTH, localDateTime.getDayOfMonth());
            cal.set(Calendar.HOUR_OF_DAY, localDateTime.getHour());
            cal.set(Calendar.MINUTE, localDateTime.getMinute());
            cal.set(Calendar.SECOND, localDateTime.getSecond());
            Timestamp timestamp = new Timestamp(cal.getTimeInMillis());
            timestamp.setNanos(localDateTime.getNano());
            dtmPreparedStatement.setTimestamp(1, timestamp);
            Long value = (Long) dtmPreparedStatement.parameters.getValues()[0];
            results.add(value);
        }

        // assert
        List<LocalDateTime> result = results.stream().map(DateTimeUtils::toLocalDateTime)
                .collect(Collectors.toList());
        assertThat(result, contains(
                is(LocalDateTime.of(2020, 1, 1, 1, 1, 11, 123_456_000)),
                is(LocalDateTime.of(1990, 6, 6, 5, 5, 11, 123_456_000)),
                is(LocalDateTime.of(1900, 1, 1, 9, 9, 11, 123_456_000)),
                is(LocalDateTime.of(1500, 5, 12, 12, 12, 30, 123_456_000)),
                is(LocalDateTime.of(1000, 6, 11, 16, 16, 40, 123_456_000)),
                is(LocalDateTime.of(123, 11, 20, 23, 23, 50, 123_456_000))
        ));
    }

    @Test
    void shouldExecuteWithSingleQueryAndIterateResults() throws SQLException {
        // arrange
        when(queryExecutor.createQuery(Mockito.any())).thenReturn(Collections.singletonList(new Query("SELECT * FROM tbl;", false)));
        doAnswer(invocationOnMock -> {
            Query query = invocationOnMock.getArgument(0);
            ResultHandler resultHandler = invocationOnMock.getArgument(2);
            List<Tuple> tuples = Arrays.asList(TestUtils.tuple(1L), TestUtils.tuple(2L), TestUtils.tuple(3L));
            Field field = new Field("id", ColumnType.BIGINT);
            resultHandler.handleResultRows(query, new Field[]{field}, tuples);
            return null;
        }).when(queryExecutor).execute(any(Query.class), any(), any());

        // act
        boolean hasResultSet = dtmPreparedStatement.execute();

        // assert
        assertTrue(hasResultSet);
        ResultSet resultSet = dtmPreparedStatement.getResultSet();
        List<Long> results = new ArrayList<>();
        while (resultSet.next()) {
            results.add(resultSet.getLong("id"));
        }
        assertThat(results, contains(1L, 2L, 3L));
    }

    @Test
    void shouldExecuteWithMultipleQueryAndIterateResults() throws SQLException {
        // arrange
        when(queryExecutor.createQuery(Mockito.any())).thenReturn(Arrays.asList(new Query("SELECT * FROM tbl;", false),
                new Query("SELECT * FROM tbl2;", false)));
        doAnswer(invocationOnMock -> {
            List<Query> query = invocationOnMock.getArgument(0);
            ResultHandler resultHandler = invocationOnMock.getArgument(2);
            List<Tuple> tuples = Arrays.asList(TestUtils.tuple(1L), TestUtils.tuple(2L), TestUtils.tuple(3L));
            List<Tuple> tuples2 = Arrays.asList(TestUtils.tuple(4L), TestUtils.tuple(5L), TestUtils.tuple(6L));
            Field id = new Field("id", ColumnType.BIGINT);
            resultHandler.handleResultRows(query.get(0), new Field[]{id}, tuples);
            resultHandler.handleResultRows(query.get(1), new Field[]{id}, tuples2);
            return null;
        }).when(queryExecutor).execute(anyList(), any(), any());

        // act
        boolean hasResultSet = dtmPreparedStatement.execute();

        // assert
        assertTrue(hasResultSet);
        ResultSet resultSet = dtmPreparedStatement.getResultSet();
        List<Long> results = new ArrayList<>();
        while (resultSet.next()) {
            results.add(resultSet.getLong("id"));
        }

        boolean moreResults = dtmPreparedStatement.getMoreResults();
        assertTrue(moreResults);
        resultSet = dtmPreparedStatement.getResultSet();
        while (resultSet.next()) {
            results.add(resultSet.getLong("id"));
        }

        assertThat(results, contains(1L, 2L, 3L, 4L, 5L, 6L));
    }

    @Test
    void shouldExecuteWithMultipleQueryAndIterateResultsAndExecuteQuery() throws SQLException {
        // arrange
        when(queryExecutor.createQuery(Mockito.any())).thenReturn(Collections.singletonList(new Query("SELECT * FROM tbl;", false)));
        doAnswer(invocationOnMock -> {
            Query query = invocationOnMock.getArgument(0);
            ResultHandler resultHandler = invocationOnMock.getArgument(2);
            List<Tuple> tuples = Arrays.asList(TestUtils.tuple(1L), TestUtils.tuple(2L), TestUtils.tuple(3L));
            Field field = new Field("id", ColumnType.BIGINT);
            resultHandler.handleResultRows(query, new Field[]{field}, tuples);
            return null;
        }).when(queryExecutor).execute(any(Query.class), any(), any());

        // act
        ResultSet resultSet = dtmPreparedStatement.executeQuery();

        // assert
        assertNotNull(resultSet);
        List<Long> results = new ArrayList<>();
        while (resultSet.next()) {
            results.add(resultSet.getLong("id"));
        }
        assertThat(results, contains(1L, 2L, 3L));
    }

    @Test
    void shouldReturnFalseWhenNoResultsAfterExecute() throws SQLException {
        // arrange
        when(queryExecutor.createQuery(Mockito.any())).thenReturn(Collections.singletonList(new Query("SELECT * FROM tbl;", false)));
        doAnswer(invocationOnMock -> null).when(queryExecutor).execute(any(Query.class), any(), any());

        // act
        boolean hasResultSet = dtmPreparedStatement.execute();

        // assert
        assertFalse(hasResultSet);
        ResultSet resultSet = dtmPreparedStatement.getResultSet();
        assertNull(resultSet);
    }

    @Test
    void shouldThrowWhenAfterExecuteResultHasException() throws SQLException {
        // arrange
        when(queryExecutor.createQuery(Mockito.any())).thenReturn(Collections.singletonList(new Query("SELECT * FROM tbl;", false)));
        doAnswer(invocationOnMock -> {
            ResultHandler resultHandler = invocationOnMock.getArgument(2);
            resultHandler.handleError(new SQLException("Exception"));
            return null;
        }).when(queryExecutor).execute(any(Query.class), any(), any());

        // act assert
        assertThrows(SQLException.class, () -> dtmPreparedStatement.execute());
    }

    @Test
    void shouldExecuteUpdate() throws SQLException {
        // arrange
        when(queryExecutor.createQuery(Mockito.any())).thenReturn(Collections.singletonList(new Query("SELECT * FROM tbl;", false)));

        // act
        int executed = dtmPreparedStatement.executeUpdate();

        // assert
        assertEquals(-1, executed);
        verify(queryExecutor).execute(any(Query.class), any(), any());
    }

    @Test
    void shouldClearParameter() throws SQLException, MalformedURLException {
        // arrange
        when(queryExecutor.createQuery(Mockito.any())).thenReturn(Collections.singletonList(new Query("SELECT * FROM tbl WHERE ?;", false)));
        doAnswer(invocationOnMock -> null).when(queryExecutor).execute(any(Query.class), queryParametersCaptor.capture(), any());

        dtmPreparedStatement.setInt(1, 1);

        // act
        dtmPreparedStatement.clearParameters();
        dtmPreparedStatement.execute();

        // assert
        QueryParameters value = queryParametersCaptor.getValue();
        assertNull(value.getValues()[0]);
    }

    @Test
    void shouldGetParameterMetadata() throws SQLException {
        // arrange
        dtmPreparedStatement.setInt(1, 1);

        // act
        ParameterMetaData parameterMetaData = dtmPreparedStatement.getParameterMetaData();

        // assert
        assertNotNull(parameterMetaData);
    }

    @Test
    void shouldExecuteBatch() throws SQLException {
        // arrange
        String nativeSql = "INSERT INTO tbl VALUES (?);";
        when(queryExecutor.createQuery(Mockito.any())).thenReturn(Collections.singletonList(new Query(nativeSql, false)));
        doAnswer(invocationOnMock -> {
            List<Query> queries = invocationOnMock.getArgument(0);
            ResultHandler resultHandler = invocationOnMock.getArgument(2);
            for (Query query : queries) {
                Field field = new Field("id", ColumnType.BIGINT);
                resultHandler.handleResultRows(query, new Field[]{field}, new ArrayList<>());
            }
            return null;
        }).when(queryExecutor).execute(queryListCaptor.capture(), parametersListCaptor.capture(), any());

        dtmPreparedStatement.setLong(1, 1L);
        dtmPreparedStatement.addBatch();
        dtmPreparedStatement.setLong(1, 2L);
        dtmPreparedStatement.addBatch();
        dtmPreparedStatement.setLong(1, 3L);
        dtmPreparedStatement.addBatch();

        // act
        int[] ints = dtmPreparedStatement.executeBatch();

        // assert
        assertEquals(3, ints.length);
        assertThat(parametersListCaptor.getValue(), contains(
                hasProperty("values", array(is(1L))),
                hasProperty("values", array(is(2L))),
                hasProperty("values", array(is(3L)))
        ));
        assertThat(queryListCaptor.getValue(), contains(
                hasProperty("nativeSql", is(nativeSql)),
                hasProperty("nativeSql", is(nativeSql)),
                hasProperty("nativeSql", is(nativeSql))
        ));
    }

    @Test
    void shouldThrowWhenExecuteWithSql() {
        // act assert
        SQLException err = assertThrows(SQLException.class, () -> dtmPreparedStatement.execute("sql"));
        assertEquals("Can't use query methods that take a query string on a prepared statement", err.getMessage());
    }

    @Test
    void shouldThrowWhenExecuteUpdateWithSql() {
        // act assert
        SQLException err = assertThrows(SQLException.class, () -> dtmPreparedStatement.executeUpdate("sql"));
        assertEquals("Can't use query methods that take a query string on a prepared statement", err.getMessage());
    }

    @Test
    void shouldThrowWhenExecuteBatchWithSql() {
        // act assert
        SQLException err = assertThrows(SQLException.class, () -> dtmPreparedStatement.addBatch("sql"));
        assertEquals("Can't use query methods that take a query string on a prepared statement", err.getMessage());
    }

    @Test
    void shouldThrowWhenExecuteQueryWithSql() {
        // act assert
        SQLException err3 = assertThrows(SQLException.class, () -> dtmPreparedStatement.executeQuery("sql"));
        assertEquals("Can't use query methods that take a query string on a prepared statement", err3.getMessage());
    }

    @Test
    void shouldSetEveryParameter() throws SQLException, MalformedURLException {
        // arrange
        when(queryExecutor.createQuery(Mockito.any())).thenReturn(Collections.singletonList(new Query("SELECT * FROM tbl WHERE ?;", false)));
        doAnswer(invocationOnMock -> null).when(queryExecutor).execute(any(Query.class), queryParametersCaptor.capture(), any());

        String streamOrReaderResult = "1234567890";
        boolean valBoolean = true;
        byte valByte = 1;
        short valShort = 2;
        int valInt = 3;
        long valLong = 4;
        float valFloat = 5.0f;
        double valDouble = 6.0f;
        BigDecimal valBigDecimal = BigDecimal.valueOf(7);
        String valString = "string";
        byte[] valBytes = new byte[]{8, 8, 8, 8};
        Date valDate = new Date(0);
        Time valTime = new Time(0);
        char valCharacter = 'a';
        Timestamp valTimestamp = new Timestamp(0);
        InputStream valAsciiInputStream1 = new ByteArrayInputStream("1234567890".getBytes(StandardCharsets.US_ASCII));
        InputStream valAsciiInputStream2 = new ByteArrayInputStream("1234567890".getBytes(StandardCharsets.US_ASCII));
        InputStream valAsciiInputStream3 = new ByteArrayInputStream("1234567890".getBytes(StandardCharsets.US_ASCII));
        InputStream valUnicodeInputStream = new ByteArrayInputStream("1234567890".getBytes(StandardCharsets.UTF_8));
        InputStream valBinaryStream1 = new ByteArrayInputStream("1234567890".getBytes(StandardCharsets.UTF_8));
        InputStream valBinaryStream2 = new ByteArrayInputStream("1234567890".getBytes(StandardCharsets.UTF_8));
        InputStream valBinaryStream3 = new ByteArrayInputStream("1234567890".getBytes(StandardCharsets.UTF_8));
        Reader valCharacterReader1 = new InputStreamReader(new ByteArrayInputStream("1234567890".getBytes(StandardCharsets.UTF_8)));
        Reader valCharacterReader2 = new InputStreamReader(new ByteArrayInputStream("1234567890".getBytes(StandardCharsets.UTF_8)));
        Reader valCharacterReader3 = new InputStreamReader(new ByteArrayInputStream("1234567890".getBytes(StandardCharsets.UTF_8)));
        Object valObject = "test";
        Ref valRef = mock(Ref.class);
        Blob valBlob = mock(Blob.class);
        InputStream valBlob2 = new ByteArrayInputStream("1234567890".getBytes(StandardCharsets.UTF_8));
        Clob valClob = mock(Clob.class);
        Reader valClob2 = new InputStreamReader(new ByteArrayInputStream("1234567890".getBytes(StandardCharsets.UTF_8)));
        Array valArray = mock(Array.class);
        URL valUrl = new URL("http://localhost");
        RowId valRowid = mock(RowId.class);
        String valNString = "nString";
        Reader valNCharacterStream = new InputStreamReader(new ByteArrayInputStream("1234567890".getBytes(StandardCharsets.UTF_8)));
        NClob valNClob = mock(NClob.class);
        Reader valNclob2 = new InputStreamReader(new ByteArrayInputStream("1234567890".getBytes(StandardCharsets.UTF_8)));
        SQLXML valSQLXML = mock(SQLXML.class);

        // Test cases for all set<Parameter> methods
        // name - to identify case in case failure
        // expectedResult - can be Matcher or simple value or Exception in case of expected exception
        // setter - is actual setter of the parameter in prepared statement
        List<NamedParameterSetter> paramSetters = Arrays.asList(
                new NamedParameterSetter("setNull1", null, id -> dtmPreparedStatement.setNull(id, Types.BOOLEAN)),
                new NamedParameterSetter("setNull2", SQLFeatureNotSupportedException.class, id -> dtmPreparedStatement.setNull(id, Types.BOOLEAN, "BOOLEAN")),
                new NamedParameterSetter("setBoolean", valBoolean, id -> dtmPreparedStatement.setBoolean(id, valBoolean)),
                new NamedParameterSetter("setByte", (short) valByte, id -> dtmPreparedStatement.setByte(id, valByte)),
                new NamedParameterSetter("setShort", valShort, id -> dtmPreparedStatement.setShort(id, valShort)),
                new NamedParameterSetter("setInt", valInt, id -> dtmPreparedStatement.setInt(id, valInt)),
                new NamedParameterSetter("setLong", valLong, id -> dtmPreparedStatement.setLong(id, valLong)),
                new NamedParameterSetter("setFloat", valFloat, id -> dtmPreparedStatement.setFloat(id, valFloat)),
                new NamedParameterSetter("setDouble", valDouble, id -> dtmPreparedStatement.setDouble(id, valDouble)),
                new NamedParameterSetter("setBigDecimal", DtmSqlException.class, id -> dtmPreparedStatement.setBigDecimal(id, valBigDecimal)),
                new NamedParameterSetter("setString", valString, id -> dtmPreparedStatement.setString(id, valString)),
                new NamedParameterSetter("setBytes", DtmSqlException.class, id -> dtmPreparedStatement.setBytes(id, valBytes)),
                new NamedParameterSetter("setDate1", instanceOf(Long.class), id -> dtmPreparedStatement.setDate(id, valDate)),
                new NamedParameterSetter("setDate2", instanceOf(Long.class), id -> dtmPreparedStatement.setDate(id, valDate, Calendar.getInstance())),
                new NamedParameterSetter("setTime1", instanceOf(Long.class), id -> dtmPreparedStatement.setTime(id, valTime)),
                new NamedParameterSetter("setTime2", instanceOf(Long.class), id -> dtmPreparedStatement.setTime(id, valTime, Calendar.getInstance())),
                new NamedParameterSetter("setTimestamp1", instanceOf(Long.class), id -> dtmPreparedStatement.setTimestamp(id, valTimestamp)),
                new NamedParameterSetter("setTimestamp2", instanceOf(Long.class), id -> dtmPreparedStatement.setTimestamp(id, valTimestamp, Calendar.getInstance())),
                new NamedParameterSetter("setAsciiStream1", SQLFeatureNotSupportedException.class, id -> dtmPreparedStatement.setAsciiStream(id, valAsciiInputStream1)),
                new NamedParameterSetter("setAsciiStream2", streamOrReaderResult, id -> dtmPreparedStatement.setAsciiStream(id, valAsciiInputStream2, 10)),
                new NamedParameterSetter("setAsciiStream3", streamOrReaderResult, id -> dtmPreparedStatement.setAsciiStream(id, valAsciiInputStream3, 10L)),
                new NamedParameterSetter("setUnicodeStream", SQLFeatureNotSupportedException.class, id -> dtmPreparedStatement.setUnicodeStream(id, valUnicodeInputStream, 10)),
                new NamedParameterSetter("setBinaryStream1", streamOrReaderResult, id -> dtmPreparedStatement.setBinaryStream(id, valBinaryStream1, 10)),
                new NamedParameterSetter("setBinaryStream2", streamOrReaderResult, id -> dtmPreparedStatement.setBinaryStream(id, valBinaryStream2, 10L)),
                new NamedParameterSetter("setBinaryStream3", SQLFeatureNotSupportedException.class, id -> dtmPreparedStatement.setBinaryStream(id, valBinaryStream3)),
                new NamedParameterSetter("setObject1", valObject, id -> dtmPreparedStatement.setObject(id, valObject)),
                new NamedParameterSetter("setObject2", valObject, id -> dtmPreparedStatement.setObject(id, valObject, Types.VARCHAR)),
                new NamedParameterSetter("setObject3", valObject, id -> dtmPreparedStatement.setObject(id, valObject, Types.VARCHAR, 10)),
                new NamedParameterSetter("setObject4", SQLFeatureNotSupportedException.class, id -> dtmPreparedStatement.setObject(id, valObject, mock(SQLType.class))),
                new NamedParameterSetter("setObject5", SQLFeatureNotSupportedException.class, id -> dtmPreparedStatement.setObject(id, valObject, mock(SQLType.class), 10)),
                new NamedParameterSetter("setCharacterStream1", SQLFeatureNotSupportedException.class, id -> dtmPreparedStatement.setCharacterStream(id, valCharacterReader1)),
                new NamedParameterSetter("setCharacterStream2", streamOrReaderResult, id -> dtmPreparedStatement.setCharacterStream(id, valCharacterReader2, 10)),
                new NamedParameterSetter("setCharacterStream3", streamOrReaderResult, id -> dtmPreparedStatement.setCharacterStream(id, valCharacterReader3, 10L)),
                new NamedParameterSetter("setRef", SQLFeatureNotSupportedException.class, id -> dtmPreparedStatement.setRef(id, valRef)),
                new NamedParameterSetter("setBlob1", SQLFeatureNotSupportedException.class, id -> dtmPreparedStatement.setBlob(id, valBlob)),
                new NamedParameterSetter("setBlob2", SQLFeatureNotSupportedException.class, id -> dtmPreparedStatement.setBlob(id, valBlob2)),
                new NamedParameterSetter("setBlob3", SQLFeatureNotSupportedException.class, id -> dtmPreparedStatement.setBlob(id, valBlob2, 10)),
                new NamedParameterSetter("setClob", SQLFeatureNotSupportedException.class, id -> dtmPreparedStatement.setClob(id, valClob)),
                new NamedParameterSetter("setClob", SQLFeatureNotSupportedException.class, id -> dtmPreparedStatement.setClob(id, valClob2)),
                new NamedParameterSetter("setClob", SQLFeatureNotSupportedException.class, id -> dtmPreparedStatement.setClob(id, valClob2, 10)),
                new NamedParameterSetter("setArray", SQLFeatureNotSupportedException.class, id -> dtmPreparedStatement.setArray(id, valArray)),
                new NamedParameterSetter("setURL", SQLFeatureNotSupportedException.class, id -> dtmPreparedStatement.setURL(id, valUrl)),
                new NamedParameterSetter("setRowId", SQLFeatureNotSupportedException.class, id -> dtmPreparedStatement.setRowId(id, valRowid)),
                new NamedParameterSetter("setNString", SQLFeatureNotSupportedException.class, id -> dtmPreparedStatement.setNString(id, valNString)),
                new NamedParameterSetter("setNCharacterStream", SQLFeatureNotSupportedException.class, id -> dtmPreparedStatement.setNCharacterStream(id, valNCharacterStream)),
                new NamedParameterSetter("setNCharacterStream", SQLFeatureNotSupportedException.class, id -> dtmPreparedStatement.setNCharacterStream(id, valNCharacterStream, 10)),
                new NamedParameterSetter("setNClob", SQLFeatureNotSupportedException.class, id -> dtmPreparedStatement.setNClob(id, valNClob)),
                new NamedParameterSetter("setNClob", SQLFeatureNotSupportedException.class, id -> dtmPreparedStatement.setNClob(id, valNclob2)),
                new NamedParameterSetter("setNClob", SQLFeatureNotSupportedException.class, id -> dtmPreparedStatement.setNClob(id, valNclob2, 10)),
                new NamedParameterSetter("setSQLXML", SQLFeatureNotSupportedException.class, id -> dtmPreparedStatement.setSQLXML(id, valSQLXML)),

                //all setObject cases
                new NamedParameterSetter("setObjectSimpleNull", is(nullValue()), id -> dtmPreparedStatement.setObject(id, null)),
                new NamedParameterSetter("setObjectSimpleVarchar", valString, id -> dtmPreparedStatement.setObject(id, valString)),
                new NamedParameterSetter("setObjectSimpleBigDecimal", DtmSqlException.class, id -> dtmPreparedStatement.setObject(id, new BigDecimal(1L))),
                new NamedParameterSetter("setObjectSimpleByte", (short) valByte, id -> dtmPreparedStatement.setObject(id, valByte)),
                new NamedParameterSetter("setObjectSimpleShort", valShort, id -> dtmPreparedStatement.setObject(id, valShort)),
                new NamedParameterSetter("setObjectSimpleInt", valInt, id -> dtmPreparedStatement.setObject(id, valInt)),
                new NamedParameterSetter("setObjectSimpleLong", valLong, id -> dtmPreparedStatement.setObject(id, valLong)),
                new NamedParameterSetter("setObjectSimpleFloat", valFloat, id -> dtmPreparedStatement.setObject(id, valFloat)),
                new NamedParameterSetter("setObjectSimpleDouble", valDouble, id -> dtmPreparedStatement.setObject(id, valDouble)),
                new NamedParameterSetter("setObjectSimpleBytes", DtmSqlException.class, id -> dtmPreparedStatement.setObject(id, valBytes)),
                new NamedParameterSetter("setObjectSimpleDate", instanceOf(Long.class), id -> dtmPreparedStatement.setObject(id, valDate)),
                new NamedParameterSetter("setObjectSimpleTime", instanceOf(Long.class), id -> dtmPreparedStatement.setObject(id, valTime)),
                new NamedParameterSetter("setObjectSimpleTimestamp", instanceOf(Long.class), id -> dtmPreparedStatement.setObject(id, valTimestamp)),
                new NamedParameterSetter("setObjectSimpleBoolean", valBoolean, id -> dtmPreparedStatement.setObject(id, valBoolean)),
                new NamedParameterSetter("setObjectSimpleCharacter", String.valueOf(valCharacter), id -> dtmPreparedStatement.setObject(id, valCharacter)),

                //all setObject expanded cases
                new NamedParameterSetter("setObjectSimpleNull", is(nullValue()), id -> dtmPreparedStatement.setObject(id, null, Types.VARCHAR, -1)),
                new NamedParameterSetter("setObjectSimpleVarchar", valString, id -> dtmPreparedStatement.setObject(id, valString, Types.VARCHAR, -1)),
                new NamedParameterSetter("setObjectSimpleBigDecimal", DtmSqlException.class, id -> dtmPreparedStatement.setObject(id, new BigDecimal(1L), Types.DECIMAL, -1)),
                new NamedParameterSetter("setObjectSimpleInt", valInt, id -> dtmPreparedStatement.setObject(id, valInt, Types.INTEGER, -1)),
                new NamedParameterSetter("setObjectSimpleLong", valLong, id -> dtmPreparedStatement.setObject(id, valLong, Types.BIGINT, -1)),
                new NamedParameterSetter("setObjectSimpleFloat", valFloat, id -> dtmPreparedStatement.setObject(id, valFloat, Types.FLOAT, -1)),
                new NamedParameterSetter("setObjectSimpleDouble", valDouble, id -> dtmPreparedStatement.setObject(id, valDouble, Types.DOUBLE, -1)),
                new NamedParameterSetter("setObjectSimpleDate", instanceOf(Long.class), id -> dtmPreparedStatement.setObject(id, valDate, Types.DATE, -1)),
                new NamedParameterSetter("setObjectSimpleTime", instanceOf(Long.class), id -> dtmPreparedStatement.setObject(id, valTime, Types.TIME, -1)),
                new NamedParameterSetter("setObjectSimpleTimestamp", instanceOf(Long.class), id -> dtmPreparedStatement.setObject(id, valTimestamp, Types.TIMESTAMP, -1)),
                new NamedParameterSetter("setObjectSimpleBoolean", valBoolean, id -> dtmPreparedStatement.setObject(id, valBoolean, Types.BOOLEAN, -1)),
                new NamedParameterSetter("setObjectSimpleCharacter", String.valueOf(valCharacter), id -> dtmPreparedStatement.setObject(id, valCharacter, Types.CHAR, -1))
        );

        dtmPreparedStatement = new DtmPreparedStatement(baseConnection, 1, 1, StringUtils.repeat("? ", paramSetters.size()));

        // act
        int id = 1;
        List<Object> expectedParams = new ArrayList<>();
        for (NamedParameterSetter paramSetter : paramSetters) {
            Object expectedResult = paramSetter.expectedResult;
            if (expectedResult instanceof Class) {
                try {
                    paramSetter.setter.accept(id);
                    fail(String.format("Expected exception: %s is not thrown in parameter: %s", ((Class<?>) expectedResult).getSimpleName(), paramSetter.name));
                } catch (Exception e) {
                    if (expectedResult != e.getClass()) {
                        fail("Incorrect exception in parameter: " + paramSetter.name, e);
                    }
                }
                continue;
            }

            try {
                paramSetter.setter.accept(id);
                expectedParams.add(expectedResult);
                id++;
            } catch (Exception e) {
                fail("Unexpected exception in parameter: " + paramSetter.name, e);
            }
        }
        dtmPreparedStatement.execute();

        // assert
        QueryParameters parameters = queryParametersCaptor.getValue();
        List<Matcher<Object>> collect = expectedParams.stream()
                .map(o -> {
                    if (o == null) {
                        return is(nullValue());
                    }

                    if (o instanceof Matcher) {
                        return (Matcher<Object>) o;
                    }

                    return is(o);
                })
                .collect(Collectors.toList());
        List<Object> limitedParams = Arrays.stream(parameters.getValues()).limit(expectedParams.size()).collect(Collectors.toList());
        assertThat(limitedParams, contains(collect));
    }

    private static class NamedParameterSetter {
        private final String name;
        private final ThrowableIntConsumer setter;
        private final Object expectedResult;

        private NamedParameterSetter(String name, Object expectedResult, ThrowableIntConsumer setter) {
            this.name = name;
            this.setter = setter;
            this.expectedResult = expectedResult;
        }
    }

    private interface ThrowableIntConsumer {
        void accept(Integer integer) throws SQLException;
    }
}