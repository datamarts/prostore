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

import ru.datamart.prostore.common.util.DateTimeUtils;
import ru.datamart.prostore.jdbc.core.BaseConnection;
import ru.datamart.prostore.jdbc.core.QueryExecutor;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DtmPreparedStatementTest {

    @Mock
    private BaseConnection baseConnection;

    @Mock
    private QueryExecutor queryExecutor;

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
                Matchers.is(LocalDate.of(2020, 1, 1)),
                Matchers.is(LocalDate.of(1990, 6, 6)),
                Matchers.is(LocalDate.of(1900, 1, 1)),
                Matchers.is(LocalDate.of(1500, 5, 12)),
                Matchers.is(LocalDate.of(1000, 6, 11)),
                Matchers.is(LocalDate.of(123, 11, 20))
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
                Matchers.is(LocalDate.of(2020, 1, 1)),
                Matchers.is(LocalDate.of(1990, 6, 6)),
                Matchers.is(LocalDate.of(1900, 1, 1)),
                Matchers.is(LocalDate.of(1500, 5, 12)),
                Matchers.is(LocalDate.of(1000, 6, 11)),
                Matchers.is(LocalDate.of(123, 11, 20))
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
                Matchers.is(LocalDate.of(2020, 1, 1)),
                Matchers.is(LocalDate.of(1990, 6, 6)),
                Matchers.is(LocalDate.of(1900, 1, 1)),
                Matchers.is(LocalDate.of(1500, 5, 12)),
                Matchers.is(LocalDate.of(1000, 6, 11)),
                Matchers.is(LocalDate.of(123, 11, 20))
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
                Matchers.is(LocalTime.of(1, 1, 11, 123_000_000)),
                Matchers.is(LocalTime.of(5, 5, 11, 123_000_000)),
                Matchers.is(LocalTime.of(9, 9, 11, 123_000_000)),
                Matchers.is(LocalTime.of(12, 12, 30, 123_000_000)),
                Matchers.is(LocalTime.of(16, 16, 40, 123_000_000)),
                Matchers.is(LocalTime.of(23, 23, 50, 123_000_000))
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
                Matchers.is(LocalTime.of(1, 1, 11, 123_000_000)),
                Matchers.is(LocalTime.of(5, 5, 11, 123_000_000)),
                Matchers.is(LocalTime.of(9, 9, 11, 123_000_000)),
                Matchers.is(LocalTime.of(12, 12, 30, 123_000_000)),
                Matchers.is(LocalTime.of(16, 16, 40, 123_000_000)),
                Matchers.is(LocalTime.of(23, 23, 50, 123_000_000))
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
                Matchers.is(LocalTime.of(1, 1, 11, 123_000_000)),
                Matchers.is(LocalTime.of(5, 5, 11, 123_000_000)),
                Matchers.is(LocalTime.of(9, 9, 11, 123_000_000)),
                Matchers.is(LocalTime.of(12, 12, 30, 123_000_000)),
                Matchers.is(LocalTime.of(16, 16, 40, 123_000_000)),
                Matchers.is(LocalTime.of(23, 23, 50, 123_000_000))
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
                Matchers.is(LocalDateTime.of(2020, 1, 1, 1, 1, 11, 123_456_000)),
                Matchers.is(LocalDateTime.of(1990, 6, 6, 5, 5, 11, 123_456_000)),
                Matchers.is(LocalDateTime.of(1900, 1, 1, 9, 9, 11, 123_456_000)),
                Matchers.is(LocalDateTime.of(1500, 5, 12, 12, 12, 30, 123_456_000)),
                Matchers.is(LocalDateTime.of(1000, 6, 11, 16, 16, 40, 123_456_000)),
                Matchers.is(LocalDateTime.of(123, 11, 20, 23, 23, 50, 123_456_000))
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
                Matchers.is(LocalDateTime.of(2020, 1, 1, 1, 1, 11, 123_456_000)),
                Matchers.is(LocalDateTime.of(1990, 6, 6, 5, 5, 11, 123_456_000)),
                Matchers.is(LocalDateTime.of(1900, 1, 1, 9, 9, 11, 123_456_000)),
                Matchers.is(LocalDateTime.of(1500, 5, 12, 12, 12, 30, 123_456_000)),
                Matchers.is(LocalDateTime.of(1000, 6, 11, 16, 16, 40, 123_456_000)),
                Matchers.is(LocalDateTime.of(123, 11, 20, 23, 23, 50, 123_456_000))
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
                Matchers.is(LocalDateTime.of(2020, 1, 1, 1, 1, 11, 123_456_000)),
                Matchers.is(LocalDateTime.of(1990, 6, 6, 5, 5, 11, 123_456_000)),
                Matchers.is(LocalDateTime.of(1900, 1, 1, 9, 9, 11, 123_456_000)),
                Matchers.is(LocalDateTime.of(1500, 5, 12, 12, 12, 30, 123_456_000)),
                Matchers.is(LocalDateTime.of(1000, 6, 11, 16, 16, 40, 123_456_000)),
                Matchers.is(LocalDateTime.of(123, 11, 20, 23, 23, 50, 123_456_000))
        ));
    }
}