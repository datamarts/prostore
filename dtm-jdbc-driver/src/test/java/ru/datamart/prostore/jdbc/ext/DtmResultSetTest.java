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

import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.datamart.prostore.common.configuration.core.CoreConstants;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.util.DateTimeUtils;
import ru.datamart.prostore.jdbc.core.*;
import ru.datamart.prostore.jdbc.utils.TestUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DtmResultSetTest {
    private final BaseConnection connection = mock(DtmConnectionImpl.class);
    private final BaseStatement statement = mock(DtmStatement.class);
    private ResultSet resultSet;
    private TimeZone INITIAL_TIMEZONE;

    @BeforeEach
    void setUp() {
        INITIAL_TIMEZONE = TimeZone.getDefault();
    }

    @AfterEach
    void cleanUp() {
        TimeZone.setDefault(INITIAL_TIMEZONE);
    }

    @Test
    void unwrap() throws SQLException {
        // arrange
        prepareResultSet(Collections.emptyList(), Collections.emptyList());

        // act assert
        assertEquals(resultSet, resultSet.unwrap(DtmResultSet.class));
        assertThrows(SQLException.class, () -> resultSet.unwrap(DtmResultSetTest.class));
    }

    @Test
    void isWrapperFor() throws SQLException {
        // arrange
        prepareResultSet(Collections.emptyList(), Collections.emptyList());

        // act assert
        assertTrue(resultSet.isWrapperFor(DtmResultSet.class));
        assertFalse(resultSet.isWrapperFor(null));
    }

    @Test
    void shouldBeSameDatesWhenUTC() throws SQLException {
        // arrange
        List<LocalDate> localDates = Arrays.asList(
                LocalDate.of(2020, 1, 1),
                LocalDate.of(1990, 6, 6),
                LocalDate.of(1900, 1, 1),
                LocalDate.of(1500, 5, 12),
                LocalDate.of(1000, 6, 11),
                LocalDate.of(123, 11, 20)
        );

        List<Tuple> tuples = localDates.stream()
                .map(localDate -> new Tuple(new Object[]{localDate.toEpochDay()}))
                .collect(Collectors.toList());

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        prepareResultSet(Arrays.asList(new Field("date", ColumnType.DATE)), tuples);

        // act
        List<String> result = new ArrayList<>();
        while (resultSet.next()) {
            Date date = resultSet.getDate("date");
            result.add(date.toString());
        }

        // assert
        assertThat(result, contains(
                is("2020-01-01"),
                is("1990-06-06"),
                is("1900-01-01"),
                is("1500-05-12"),
                is("1000-06-11"),
                is("0123-11-20")
        ));
    }

    @Test
    void shouldBeSameDateWhenGetObject() throws SQLException {
        // arrange
        List<LocalDate> localDates = Arrays.asList(
                LocalDate.of(2020, 1, 1)
        );

        List<Tuple> tuples = localDates.stream()
                .map(localDate -> new Tuple(new Object[]{localDate.toEpochDay()}))
                .collect(Collectors.toList());

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        prepareResultSet(Arrays.asList(new Field("date", ColumnType.DATE)), tuples);

        // act
        List<String> result = new ArrayList<>();
        while (resultSet.next()) {
            Object date = resultSet.getObject("date");
            result.add(date.toString());
        }

        // assert
        assertThat(result, contains(
                is("2020-01-01")
        ));
    }

    @Test
    void shouldBeSameDatesWhenClientIsShiftedGMT() throws SQLException {
        // arrange
        List<LocalDate> localDates = Arrays.asList(
                LocalDate.of(2020, 1, 1),
                LocalDate.of(1990, 6, 6),
                LocalDate.of(1900, 1, 1),
                LocalDate.of(1500, 5, 12),
                LocalDate.of(1000, 6, 11),
                LocalDate.of(123, 11, 20)
        );

        List<Tuple> tuples = localDates.stream()
                .map(localDate -> new Tuple(new Object[]{localDate.toEpochDay()}))
                .collect(Collectors.toList());

        prepareResultSet(Arrays.asList(new Field("date", ColumnType.DATE)), tuples);

        TimeZone timeZone = TimeZone.getTimeZone("GMT+11:30");
        TimeZone.setDefault(timeZone);

        // act
        List<String> result = new ArrayList<>();
        while (resultSet.next()) {
            Date date = resultSet.getDate("date", Calendar.getInstance(timeZone));
            result.add(date.toString());
        }

        // assert
        assertThat(result, contains(
                is("2020-01-01"),
                is("1990-06-06"),
                is("1900-01-01"),
                is("1500-05-12"),
                is("1000-06-11"),
                is("0123-11-20")
        ));
    }

    @Test
    void shouldBeSameDatesWhenClientIsNotUtc() throws SQLException {
        // arrange
        List<LocalDate> localDates = Arrays.asList(
                LocalDate.of(2020, 1, 1),
                LocalDate.of(1990, 6, 6),
                LocalDate.of(1900, 1, 1),
                LocalDate.of(1500, 5, 12),
                LocalDate.of(1000, 6, 11),
                LocalDate.of(123, 11, 20)
        );

        List<Tuple> tuples = localDates.stream()
                .map(localDate -> new Tuple(new Object[]{localDate.toEpochDay()}))
                .collect(Collectors.toList());

        prepareResultSet(Arrays.asList(new Field("date", ColumnType.DATE)), tuples);

        TimeZone timeZone = TimeZone.getTimeZone("Asia/Novosibirsk");
        TimeZone.setDefault(timeZone);

        // act
        List<String> result = new ArrayList<>();
        while (resultSet.next()) {
            Date date = resultSet.getDate("date", Calendar.getInstance(timeZone));
            result.add(date.toString());
        }

        // assert
        assertThat(result, contains(
                is("2020-01-01"),
                is("1990-06-06"),
                is("1900-01-01"),
                is("1500-05-12"),
                is("1000-06-11"),
                is("0123-11-20")
        ));
    }

    @Test
    void shouldBeSameTimestampsWhenUTC() throws SQLException {
        // arrange
        List<LocalDateTime> localDates = Arrays.asList(
                LocalDateTime.of(2020, 1, 1, 11, 11, 11, 123456789),
                LocalDateTime.of(1990, 6, 6, 11, 11, 11, 123456789),
                LocalDateTime.of(1900, 1, 1, 11, 11, 11, 123456789),
                LocalDateTime.of(1500, 5, 12, 11, 11, 11, 123456789),
                LocalDateTime.of(1000, 6, 11, 11, 11, 11, 123456789),
                LocalDateTime.of(123, 11, 20, 11, 11, 11, 123456789)
        );

        List<Tuple> tuples = localDates.stream()
                .map(date -> {
                    Instant instant = date.atZone(CoreConstants.CORE_TIME_ZONE.toZoneId()).toInstant();
                    return new Tuple(new Object[]{instant.getLong(ChronoField.INSTANT_SECONDS) * 1000L * 1000L + instant.getLong(ChronoField.MICRO_OF_SECOND)});
                })
                .collect(Collectors.toList());

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        prepareResultSet(Arrays.asList(new Field("timestamp", 6, ColumnType.TIMESTAMP, null)), tuples);

        // act
        List<String> result = new ArrayList<>();
        while (resultSet.next()) {
            Timestamp timestamp = resultSet.getTimestamp("timestamp");
            result.add(timestamp.toString());
        }

        // assert
        assertThat(result, contains(
                is("2020-01-01 11:11:11.123456"),
                is("1990-06-06 11:11:11.123456"),
                is("1900-01-01 11:11:11.123456"),
                is("1500-05-12 11:11:11.123456"),
                is("1000-06-11 11:11:11.123456"),
                is("0123-11-20 11:11:11.123456")
        ));
    }

    @Test
    void shouldBeSameTimestampsWhenGetObject() throws SQLException {
        // arrange
        List<LocalDateTime> localDates = Arrays.asList(
                LocalDateTime.of(2020, 1, 1, 11, 11, 11, 123456789)
        );

        List<Tuple> tuples = localDates.stream()
                .map(date -> {
                    Instant instant = date.atZone(CoreConstants.CORE_TIME_ZONE.toZoneId()).toInstant();
                    return new Tuple(new Object[]{instant.getLong(ChronoField.INSTANT_SECONDS) * 1000L * 1000L + instant.getLong(ChronoField.MICRO_OF_SECOND)});
                })
                .collect(Collectors.toList());

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        prepareResultSet(Arrays.asList(new Field("timestamp", 6, ColumnType.TIMESTAMP, null)), tuples);

        // act
        List<String> result = new ArrayList<>();
        while (resultSet.next()) {
            Object timestamp = resultSet.getObject("timestamp");
            result.add(timestamp.toString());
        }

        // assert
        assertThat(result, contains(
                is("2020-01-01 11:11:11.123456")
        ));
    }

    @Test
    void shouldBeSameTimestampsWhenShiftedGMT() throws SQLException {
        // arrange
        List<LocalDateTime> localDates = Arrays.asList(
                LocalDateTime.of(2020, 1, 1, 11, 11, 11, 123456789),
                LocalDateTime.of(1990, 6, 6, 11, 11, 11, 123456789),
                LocalDateTime.of(1900, 1, 1, 11, 11, 11, 123456789),
                LocalDateTime.of(1500, 5, 12, 11, 11, 11, 123456789),
                LocalDateTime.of(1000, 6, 11, 11, 11, 11, 123456789),
                LocalDateTime.of(123, 11, 20, 11, 11, 11, 123456789)
        );

        List<Tuple> tuples = localDates.stream()
                .map(date -> {
                    Instant instant = date.atZone(CoreConstants.CORE_TIME_ZONE.toZoneId()).toInstant();
                    return new Tuple(new Object[]{instant.getLong(ChronoField.INSTANT_SECONDS) * 1000L * 1000L + instant.getLong(ChronoField.MICRO_OF_SECOND)});
                })
                .collect(Collectors.toList());

        TimeZone.setDefault(TimeZone.getTimeZone("GMT+11:30"));
        prepareResultSet(Arrays.asList(new Field("timestamp", 6, ColumnType.TIMESTAMP, null)), tuples);

        // act
        List<String> result = new ArrayList<>();
        while (resultSet.next()) {
            Timestamp timestamp = resultSet.getTimestamp("timestamp");
            result.add(timestamp.toString());
        }

        // assert
        assertThat(result, contains(
                is("2020-01-01 11:11:11.123456"),
                is("1990-06-06 11:11:11.123456"),
                is("1900-01-01 11:11:11.123456"),
                is("1500-05-12 11:11:11.123456"),
                is("1000-06-11 11:11:11.123456"),
                is("0123-11-20 11:11:11.123456")
        ));
    }

    @Test
    void shouldBeSameTimestampsWhenNotUTC() throws SQLException {
        // arrange
        List<LocalDateTime> localDates = Arrays.asList(
                LocalDateTime.of(2020, 1, 1, 11, 11, 11, 123456789),
                LocalDateTime.of(1990, 6, 6, 11, 11, 11, 123456789),
                LocalDateTime.of(1900, 1, 1, 11, 11, 11, 123456789),
                LocalDateTime.of(1500, 5, 12, 11, 11, 11, 123456789),
                LocalDateTime.of(1000, 6, 11, 11, 11, 11, 123456789),
                LocalDateTime.of(123, 11, 20, 11, 11, 11, 123456789)
        );

        List<Tuple> tuples = localDates.stream()
                .map(date -> {
                    Instant instant = date.atZone(CoreConstants.CORE_TIME_ZONE.toZoneId()).toInstant();
                    return new Tuple(new Object[]{instant.getLong(ChronoField.INSTANT_SECONDS) * 1000L * 1000L + instant.getLong(ChronoField.MICRO_OF_SECOND)});
                })
                .collect(Collectors.toList());

        prepareResultSet(Arrays.asList(new Field("timestamp", 6, ColumnType.TIMESTAMP, null)), tuples);

        TimeZone timeZone = TimeZone.getTimeZone("Asia/Novosibirsk");
        TimeZone.setDefault(timeZone);

        // act
        List<String> result = new ArrayList<>();
        while (resultSet.next()) {
            Timestamp timestamp = resultSet.getTimestamp("timestamp", Calendar.getInstance(timeZone));
            result.add(timestamp.toString());
        }

        // assert
        assertThat(result, contains(
                is("2020-01-01 11:11:11.123456"),
                is("1990-06-06 11:11:11.123456"),
                is("1900-01-01 11:11:11.123456"),
                is("1500-05-12 11:11:11.123456"),
                is("1000-06-11 11:11:11.123456"),
                is("0123-11-20 11:11:11.123456")
        ));
    }

    @Test
    void shouldBeSameTimeWhenUTC() throws SQLException {
        // arrange
        List<LocalTime> localDates = Arrays.asList(
                LocalTime.of(1, 1, 11),
                LocalTime.of(5, 5, 11),
                LocalTime.of(9, 9, 11),
                LocalTime.of(12, 12, 30),
                LocalTime.of(16, 16, 40),
                LocalTime.of(23, 23, 50)
        );

        List<Tuple> tuples = localDates.stream()
                .map(date -> new Tuple(new Object[]{date.toNanoOfDay() / 1000}))
                .collect(Collectors.toList());

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        prepareResultSet(Arrays.asList(new Field("time", ColumnType.TIME)), tuples);

        // act
        List<String> result = new ArrayList<>();
        while (resultSet.next()) {
            Time timestamp = resultSet.getTime("time");
            result.add(timestamp.toString());
        }

        // assert
        assertThat(result, contains(
                is("01:01:11"),
                is("05:05:11"),
                is("09:09:11"),
                is("12:12:30"),
                is("16:16:40"),
                is("23:23:50")
        ));
    }

    @Test
    void shouldBeSameTimeWhenGetObject() throws SQLException {
        // arrange
        List<LocalTime> localDates = Arrays.asList(
                LocalTime.of(1, 1, 11)
        );

        List<Tuple> tuples = localDates.stream()
                .map(date -> new Tuple(new Object[]{date.toNanoOfDay() / 1000}))
                .collect(Collectors.toList());

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        prepareResultSet(Arrays.asList(new Field("time", ColumnType.TIME)), tuples);

        // act
        List<String> result = new ArrayList<>();
        while (resultSet.next()) {
            Object timestamp = resultSet.getObject("time");
            result.add(timestamp.toString());
        }

        // assert
        assertThat(result, contains(
                is("01:01:11")
        ));
    }

    @Test
    void shouldBeSameTimeWhenShiftedGMT() throws SQLException {
        // arrange
        List<LocalTime> localDates = Arrays.asList(
                LocalTime.of(1, 1, 11),
                LocalTime.of(5, 5, 11),
                LocalTime.of(9, 9, 11),
                LocalTime.of(12, 12, 30),
                LocalTime.of(16, 16, 40),
                LocalTime.of(23, 23, 50)
        );

        List<Tuple> tuples = localDates.stream()
                .map(date -> new Tuple(new Object[]{date.toNanoOfDay() / 1000}))
                .collect(Collectors.toList());
        prepareResultSet(Arrays.asList(new Field("time", ColumnType.TIME)), tuples);

        TimeZone timeZone = TimeZone.getTimeZone("GMT+11:30");
        TimeZone.setDefault(timeZone);

        // act
        List<String> result = new ArrayList<>();
        while (resultSet.next()) {
            Time timestamp = resultSet.getTime("time", Calendar.getInstance(timeZone));
            result.add(timestamp.toString());
        }

        // assert
        assertThat(result, contains(
                is("01:01:11"),
                is("05:05:11"),
                is("09:09:11"),
                is("12:12:30"),
                is("16:16:40"),
                is("23:23:50")
        ));
    }

    @Test
    void shouldBeSameTimeWhenNotUTC() throws SQLException {
        // arrange
        List<LocalTime> localDates = Arrays.asList(
                LocalTime.of(1, 1, 11),
                LocalTime.of(5, 5, 11),
                LocalTime.of(9, 9, 11),
                LocalTime.of(12, 12, 30),
                LocalTime.of(16, 16, 40),
                LocalTime.of(23, 23, 50)
        );

        List<Tuple> tuples = localDates.stream()
                .map(date -> new Tuple(new Object[]{date.toNanoOfDay() / 1000}))
                .collect(Collectors.toList());
        prepareResultSet(Arrays.asList(new Field("time", ColumnType.TIME)), tuples);

        TimeZone timeZone = TimeZone.getTimeZone("Asia/Novosibirsk");
        TimeZone.setDefault(timeZone);

        // act
        List<String> result = new ArrayList<>();
        while (resultSet.next()) {
            Time timestamp = resultSet.getTime("time", Calendar.getInstance(timeZone));
            result.add(timestamp.toString());
        }

        // assert
        assertThat(result, contains(
                is("01:01:11"),
                is("05:05:11"),
                is("09:09:11"),
                is("12:12:30"),
                is("16:16:40"),
                is("23:23:50")
        ));
    }

    @Test
    void shouldReturnFalseOnNextWhenEmpty() throws SQLException {
        // arrange
        prepareResultSet(Collections.emptyList(), Collections.emptyList());

        // act assert
        assertFalse(resultSet.next());
    }

    @Test
    void shouldReturnTrueOnNext() throws SQLException {
        // arrange
        prepareResultSet(Arrays.asList(new Field("val", ColumnType.BIGINT)), Arrays.asList(TestUtils.tuple(1L)));

        // act
        boolean next = resultSet.next();

        // assert
        assertTrue(next);
        assertNotNull(resultSet.getObject(1));
    }

    @Test
    void shouldReturnFalseOnFirstWhenEmpty() throws SQLException {
        // arrange
        prepareResultSet(Collections.emptyList(), Collections.emptyList());

        // act assert
        assertFalse(resultSet.first());
    }

    @Test
    void shouldReturnTrueOnFirst() throws SQLException {
        // arrange
        prepareResultSet(Arrays.asList(new Field("val", ColumnType.BIGINT)), Arrays.asList(TestUtils.tuple(1L)));

        // act
        boolean first = resultSet.first();

        // assert
        assertTrue(first);
        assertNotNull(resultSet.getObject(1));
    }

    @Test
    void testVarcharColumn() throws SQLException {
        // arrange
        prepareAllTypesResultSet();

        // act
        String byIndex = resultSet.getString(ColumnType.VARCHAR.ordinal() + 1);
        String byColumn = resultSet.getString("VARCHAR");
        String byObjectIndex = (String) resultSet.getObject(ColumnType.VARCHAR.ordinal() + 1);
        String byObjectColumn = (String) resultSet.getObject("VARCHAR");

        // assert
        assertEquals("VARCHAR", byIndex);
        assertEquals("VARCHAR", byColumn);
        assertEquals("VARCHAR", byObjectIndex);
        assertEquals("VARCHAR", byObjectColumn);
    }

    @Test
    void testIntColumn() throws SQLException {
        // arrange
        prepareAllTypesResultSet();

        // act
        long longByIndex = resultSet.getLong(ColumnType.INT.ordinal() + 1);
        long longByColumn = resultSet.getLong("INT");
        int intByIndex = resultSet.getInt(ColumnType.INT.ordinal() + 1);
        int intByColumn = resultSet.getInt("INT");
        Object byObjectIndex = resultSet.getObject(ColumnType.INT.ordinal() + 1);
        Object byObjectColumn = resultSet.getObject("INT");
        byte byteByIndex = resultSet.getByte(ColumnType.INT.ordinal() + 1);
        byte byteByColumn = resultSet.getByte("INT");
        short shortByIndex = resultSet.getShort(ColumnType.INT.ordinal() + 1);
        short shortByColumn = resultSet.getShort("INT");

        // assert
        assertEquals(1L, longByIndex);
        assertEquals(1L, longByColumn);
        assertEquals(1, intByIndex);
        assertEquals(1, intByColumn);
        assertEquals(1L, byObjectIndex);
        assertEquals(1L, byObjectColumn);
        assertEquals(1, byteByIndex);
        assertEquals(1, byteByColumn);
        assertEquals(1, shortByIndex);
        assertEquals(1, shortByColumn);
    }

    @Test
    void testBigintColumn() throws SQLException {
        // arrange
        prepareAllTypesResultSet();

        // act
        long longByIndex = resultSet.getLong(ColumnType.BIGINT.ordinal() + 1);
        long longByColumn = resultSet.getLong("BIGINT");
        int intByIndex = resultSet.getInt(ColumnType.BIGINT.ordinal() + 1);
        int intByColumn = resultSet.getInt("BIGINT");
        Object byObjectIndex = resultSet.getObject(ColumnType.BIGINT.ordinal() + 1);
        Object byObjectColumn = resultSet.getObject("BIGINT");
        byte byteByIndex = resultSet.getByte(ColumnType.BIGINT.ordinal() + 1);
        byte byteByColumn = resultSet.getByte("BIGINT");
        short shortByIndex = resultSet.getShort(ColumnType.BIGINT.ordinal() + 1);
        short shortByColumn = resultSet.getShort("BIGINT");

        // assert
        assertEquals(2L, longByIndex);
        assertEquals(2L, longByColumn);
        assertEquals(2, intByIndex);
        assertEquals(2, intByColumn);
        assertEquals(2L, byObjectIndex);
        assertEquals(2L, byObjectColumn);
        assertEquals(2, byteByIndex);
        assertEquals(2, byteByColumn);
        assertEquals(2, shortByIndex);
        assertEquals(2, shortByColumn);
    }

    @Test
    void testInt32Column() throws SQLException {
        // arrange
        prepareAllTypesResultSet();

        // act
        long longByIndex = resultSet.getLong(ColumnType.INT32.ordinal() + 1);
        long longByColumn = resultSet.getLong("INT32");
        int intByIndex = resultSet.getInt(ColumnType.INT32.ordinal() + 1);
        int intByColumn = resultSet.getInt("INT32");
        Object byObjectIndex = resultSet.getObject(ColumnType.INT32.ordinal() + 1);
        Object byObjectColumn = resultSet.getObject("INT32");
        byte byteByIndex = resultSet.getByte(ColumnType.INT32.ordinal() + 1);
        byte byteByColumn = resultSet.getByte("INT32");
        short shortByIndex = resultSet.getShort(ColumnType.INT32.ordinal() + 1);
        short shortByColumn = resultSet.getShort("INT32");

        // assert
        assertEquals(3L, longByIndex);
        assertEquals(3L, longByColumn);
        assertEquals(3, intByIndex);
        assertEquals(3, intByColumn);
        assertEquals(3, byObjectIndex);
        assertEquals(3, byObjectColumn);
        assertEquals(3, byteByIndex);
        assertEquals(3, byteByColumn);
        assertEquals(3, shortByIndex);
        assertEquals(3, shortByColumn);
    }

    @Test
    void shouldReturnCorrectIndex() throws SQLException {
        // arrange
        prepareAllTypesResultSet();

        // act assert
        for (ColumnType type : ColumnType.values()) {
            int column = resultSet.findColumn(type.name());
            assertEquals(type.ordinal() + 1, column);
        }
    }

    @Test
    void shouldClose() throws SQLException {
        // arrange
        prepareAllTypesResultSet();

        // act
        resultSet.close();
    }

    @Test
    void testAnyColumn() throws SQLException {
        // arrange
        prepareAllTypesResultSet();

        // act
        String byIndex = resultSet.getString(ColumnType.ANY.ordinal() + 1);
        String byColumn = resultSet.getString("ANY");
        String byObjectIndex = (String) resultSet.getObject(ColumnType.ANY.ordinal() + 1);
        String byObjectColumn = (String) resultSet.getObject("ANY");

        // assert
        assertEquals("ANY", byIndex);
        assertEquals("ANY", byColumn);
        assertEquals("ANY", byObjectIndex);
        assertEquals("ANY", byObjectColumn);
    }

    @Test
    void testCharColumn() throws SQLException {
        // arrange
        prepareAllTypesResultSet();

        // act
        String byIndex = resultSet.getString(ColumnType.CHAR.ordinal() + 1);
        String byColumn = resultSet.getString("CHAR");
        String byObjectIndex = (String) resultSet.getObject(ColumnType.CHAR.ordinal() + 1);
        String byObjectColumn = (String) resultSet.getObject("CHAR");

        // assert
        assertEquals("CHAR", byIndex);
        assertEquals("CHAR", byColumn);
        assertEquals("CHAR", byObjectIndex);
        assertEquals("CHAR", byObjectColumn);
    }

    @Test
    void testUuidColumn() throws SQLException {
        // arrange
        prepareAllTypesResultSet();

        // act
        String byIndex = resultSet.getString(ColumnType.UUID.ordinal() + 1);
        String byColumn = resultSet.getString("UUID");
        String byObjectIndex = (String) resultSet.getObject(ColumnType.UUID.ordinal() + 1);
        String byObjectColumn = (String) resultSet.getObject("UUID");

        // assert
        assertEquals("UUID", byIndex);
        assertEquals("UUID", byColumn);
        assertEquals("UUID", byObjectIndex);
        assertEquals("UUID", byObjectColumn);
    }

    @Test
    void testBlobColumn() throws SQLException {
        // arrange
        prepareAllTypesResultSet();

        // act
        String byIndex = resultSet.getString(ColumnType.BLOB.ordinal() + 1);
        String byColumn = resultSet.getString("BLOB");
        String byObjectIndex = (String) resultSet.getObject(ColumnType.BLOB.ordinal() + 1);
        String byObjectColumn = (String) resultSet.getObject("BLOB");

        // assert
        assertEquals("BLOB", byIndex);
        assertEquals("BLOB", byColumn);
        assertEquals("BLOB", byObjectIndex);
        assertEquals("BLOB", byObjectColumn);
    }

    @Test
    void testLinkColumn() throws SQLException {
        // arrange
        prepareAllTypesResultSet();

        // act
        String byIndex = resultSet.getString(ColumnType.LINK.ordinal() + 1);
        String byColumn = resultSet.getString("LINK");
        String byObjectIndex = (String) resultSet.getObject(ColumnType.LINK.ordinal() + 1);
        String byObjectColumn = (String) resultSet.getObject("LINK");

        // assert
        assertEquals("LINK", byIndex);
        assertEquals("LINK", byColumn);
        assertEquals("LINK", byObjectIndex);
        assertEquals("LINK", byObjectColumn);
    }

    @Test
    void testFloatColumn() throws SQLException {
        // arrange
        prepareAllTypesResultSet();

        // act
        float byIndex = resultSet.getFloat(ColumnType.FLOAT.ordinal() + 1);
        float byColumn = resultSet.getFloat("FLOAT");
        float byObjectIndex = (Float) resultSet.getObject(ColumnType.FLOAT.ordinal() + 1);
        float byObjectColumn = (Float) resultSet.getObject("FLOAT");

        // assert
        assertEquals(1.1f, byIndex);
        assertEquals(1.1f, byColumn);
        assertEquals(1.1f, byObjectIndex);
        assertEquals(1.1f, byObjectColumn);
    }

    @Test
    void testDoubleColumn() throws SQLException {
        // arrange
        prepareAllTypesResultSet();

        // act
        double byIndex = resultSet.getDouble(ColumnType.DOUBLE.ordinal() + 1);
        double byColumn = resultSet.getDouble("DOUBLE");
        double byObjectIndex = (Double) resultSet.getObject(ColumnType.DOUBLE.ordinal() + 1);
        double byObjectColumn = (Double) resultSet.getObject("DOUBLE");

        // assert
        assertEquals(1.2, byIndex);
        assertEquals(1.2, byColumn);
        assertEquals(1.2, byObjectIndex);
        assertEquals(1.2, byObjectColumn);
    }

    @Test
    void testBooleanColumn() throws SQLException {
        // arrange
        prepareAllTypesResultSet();

        // act
        boolean byIndex = resultSet.getBoolean(ColumnType.BOOLEAN.ordinal() + 1);
        boolean byColumn = resultSet.getBoolean("BOOLEAN");
        boolean byObjectIndex = (Boolean) resultSet.getObject(ColumnType.BOOLEAN.ordinal() + 1);
        boolean byObjectColumn = (Boolean) resultSet.getObject("BOOLEAN");

        // assert
        assertTrue(byIndex);
        assertTrue(byColumn);
        assertTrue(byObjectIndex);
        assertTrue(byObjectColumn);
    }

    @Test
    void testConvertNumericTypes() throws SQLException {
        // arrange
        prepareAllTypesResultSet();

        // act
        byte byteVal = resultSet.getByte(ColumnType.INT32.name());
        short shortVal = resultSet.getShort(ColumnType.INT32.name());
        int intVal = resultSet.getInt(ColumnType.BIGINT.name());
        long longVal = resultSet.getLong(ColumnType.INT32.name());
        float floatVal = resultSet.getFloat(ColumnType.INT32.name());
        double doubleVal = resultSet.getDouble(ColumnType.INT32.name());

        // assert
        assertEquals(3, byteVal);
        assertEquals(3, shortVal);
        assertEquals(2, intVal);
        assertEquals(3, longVal);
        assertEquals(3, floatVal);
        assertEquals(3, doubleVal);
    }

    @Test
    void shouldThrowClassCastExceptionWhenIncorrectType() {
        // arrange
        prepareAllTypesResultSet();

        // act assert
        assertThrows(ClassCastException.class, () -> resultSet.getByte(ColumnType.BOOLEAN.name()));
        assertThrows(ClassCastException.class, () -> resultSet.getShort(ColumnType.BOOLEAN.name()));
        assertThrows(ClassCastException.class, () -> resultSet.getInt(ColumnType.BOOLEAN.name()));
        assertThrows(ClassCastException.class, () -> resultSet.getLong(ColumnType.BOOLEAN.name()));
        assertThrows(ClassCastException.class, () -> resultSet.getFloat(ColumnType.BOOLEAN.name()));
        assertThrows(ClassCastException.class, () -> resultSet.getDouble(ColumnType.BOOLEAN.name()));
        assertThrows(ClassCastException.class, () -> resultSet.getDate(ColumnType.BOOLEAN.name()));
        assertThrows(ClassCastException.class, () -> resultSet.getTimestamp(ColumnType.BOOLEAN.name()));
        assertThrows(ClassCastException.class, () -> resultSet.getTime(ColumnType.BOOLEAN.name()));
        assertThrows(ClassCastException.class, () -> resultSet.getBoolean(ColumnType.INT.name()));
    }

    @Test
    void getAsciiStream() throws SQLException, IOException {
        //arrange
        prepareAllTypesResultSet();

        // act assert
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(resultSet.getAsciiStream(ColumnType.VARCHAR.name())))) {
            assertEquals("VARCHAR", bufferedReader.readLine());
        }
    }

    @Test
    void getBinaryStream() throws SQLException, IOException {
        //arrange
        prepareAllTypesResultSet();

        // act assert
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(resultSet.getBinaryStream(ColumnType.VARCHAR.name())))) {
            assertEquals("VARCHAR", bufferedReader.readLine());
        }
    }

    @Test
    void testAllMethodsSetNullForNullValues() throws SQLException {
        // arrange
        List<Field> fields = new ArrayList<>();
        List<Object> values1 = new ArrayList<>();
        for (ColumnType type : ColumnType.values()) {
            Field e = new Field(type.name(), type);

            if (type == ColumnType.VARCHAR || type == ColumnType.TIMESTAMP) {
                e.setSize(6);
            }

            fields.add(e);
            values1.add(null);
        }

        List<Object> values2 = new ArrayList<>();
        for (ColumnType type : ColumnType.values()) {
            Field e = new Field(type.name(), type);

            if (type == ColumnType.VARCHAR || type == ColumnType.TIMESTAMP) {
                e.setSize(6);
            }

            fields.add(e);
            values2.add(getObjectByType(type));
        }

        prepareResultSet(fields, Arrays.asList(new Tuple(values1.toArray(new Object[0])), new Tuple(values2.toArray(new Object[0]))));

        for (ColumnType type : ColumnType.values()) {
            // act 1
            resultSet.first();
            Object r1 = resultSet.getObject(type.name());

            // assert 1
            assertNull(r1);
            assertTrue(resultSet.wasNull());

            // act2
            resultSet.next();
            Object r2 = resultSet.getObject(type.name());
            assertNotNull(r2);
            assertFalse(resultSet.wasNull());
        }
    }

    @Test
    void testNullableFlagOnByteGetter() throws SQLException {
        // arrange
        List<Field> fields = new ArrayList<>();
        Field e = new Field("test", ColumnType.BIGINT);
        fields.add(e);
        prepareResultSet(fields, Arrays.asList(new Tuple(new Object[]{null}), new Tuple(new Object[]{1})));
        resultSet.first();

        // act 1
        byte r1 = resultSet.getByte("test");

        // assert 1
        assertEquals(0, r1);
        assertTrue(resultSet.wasNull());

        // act 2
        resultSet.next();
        byte r2 = resultSet.getByte("test");

        // assert 2
        assertEquals(1, r2);
        assertFalse(resultSet.wasNull());
    }

    @Test
    void testNullableFlagOnShortGetter() throws SQLException {
        // arrange
        List<Field> fields = new ArrayList<>();
        Field e = new Field("test", ColumnType.BIGINT);
        fields.add(e);
        prepareResultSet(fields, Arrays.asList(new Tuple(new Object[]{null}), new Tuple(new Object[]{1})));
        resultSet.first();

        // act 1
        short r1 = resultSet.getShort("test");

        // assert 1
        assertEquals(0, r1);
        assertTrue(resultSet.wasNull());

        // act 2
        resultSet.next();
        short r2 = resultSet.getShort("test");

        // assert 2
        assertEquals(1, r2);
        assertFalse(resultSet.wasNull());
    }

    @Test
    void testNullableFlagOnIntGetter() throws SQLException {
        // arrange
        List<Field> fields = new ArrayList<>();
        Field e = new Field("test", ColumnType.BIGINT);
        fields.add(e);
        prepareResultSet(fields, Arrays.asList(new Tuple(new Object[]{null}), new Tuple(new Object[]{1})));
        resultSet.first();

        // act 1
        int r1 = resultSet.getInt("test");

        // assert 1
        assertEquals(0, r1);
        assertTrue(resultSet.wasNull());

        // act 2
        resultSet.next();
        int r2 = resultSet.getInt("test");

        // assert 2
        assertEquals(1, r2);
        assertFalse(resultSet.wasNull());
    }

    @Test
    void testNullableFlagOnLongGetter() throws SQLException {
        // arrange
        List<Field> fields = new ArrayList<>();
        Field e = new Field("test", ColumnType.BIGINT);
        fields.add(e);
        prepareResultSet(fields, Arrays.asList(new Tuple(new Object[]{null}), new Tuple(new Object[]{1})));
        resultSet.first();

        // act 1
        long r1 = resultSet.getLong("test");

        // assert 1
        assertEquals(0, r1);
        assertTrue(resultSet.wasNull());

        // act 2
        resultSet.next();
        long r2 = resultSet.getLong("test");

        // assert 2
        assertEquals(1, r2);
        assertFalse(resultSet.wasNull());
    }

    @Test
    void testNullableFlagOnFloatGetter() throws SQLException {
        // arrange
        List<Field> fields = new ArrayList<>();
        Field e = new Field("test", ColumnType.FLOAT);
        fields.add(e);
        prepareResultSet(fields, Arrays.asList(new Tuple(new Object[]{null}), new Tuple(new Object[]{1f})));
        resultSet.first();

        // act 1
        float r1 = resultSet.getFloat("test");

        // assert 1
        assertEquals(0, r1);
        assertTrue(resultSet.wasNull());

        // act 2
        resultSet.next();
        float r2 = resultSet.getFloat("test");

        // assert 2
        assertEquals(1, r2);
        assertFalse(resultSet.wasNull());
    }

    @Test
    void testNullableFlagOnDoubleGetter() throws SQLException {
        // arrange
        List<Field> fields = new ArrayList<>();
        Field e = new Field("test", ColumnType.DOUBLE);
        fields.add(e);
        prepareResultSet(fields, Arrays.asList(new Tuple(new Object[]{null}), new Tuple(new Object[]{1d})));
        resultSet.first();

        // act 1
        double r1 = resultSet.getDouble("test");

        // assert 1
        assertEquals(0, r1);
        assertTrue(resultSet.wasNull());

        // act 2
        resultSet.next();
        double r2 = resultSet.getDouble("test");

        // assert 2
        assertEquals(1, r2);
        assertFalse(resultSet.wasNull());
    }

    @Test
    void testNullableFlagOnStringGetter() throws SQLException {
        // arrange
        List<Field> fields = new ArrayList<>();
        Field e = new Field("test", ColumnType.VARCHAR);
        fields.add(e);
        prepareResultSet(fields, Arrays.asList(new Tuple(new Object[]{null}), new Tuple(new Object[]{"1"})));
        resultSet.first();

        // act 1
        String r1 = resultSet.getString("test");

        // assert 1
        assertNull(r1);
        assertTrue(resultSet.wasNull());

        // act 2
        resultSet.next();
        String r2 = resultSet.getString("test");

        // assert 2
        assertEquals("1", r2);
        assertFalse(resultSet.wasNull());
    }

    @Test
    void testNullableFlagOnBooleanGetter() throws SQLException {
        // arrange
        List<Field> fields = new ArrayList<>();
        Field e = new Field("test", ColumnType.VARCHAR);
        fields.add(e);
        prepareResultSet(fields, Arrays.asList(new Tuple(new Object[]{null}), new Tuple(new Object[]{true})));
        resultSet.first();

        // act 1
        boolean r1 = resultSet.getBoolean("test");

        // assert 1
        assertFalse(r1);
        assertTrue(resultSet.wasNull());

        // act 2
        resultSet.next();
        boolean r2 = resultSet.getBoolean("test");

        // assert 2
        assertTrue(r2);
        assertFalse(resultSet.wasNull());
    }

    @Test
    void testNullableFlagOnDateGetter() throws SQLException {
        // arrange
        List<Field> fields = new ArrayList<>();
        Field e = new Field("test", ColumnType.DATE);
        fields.add(e);
        prepareResultSet(fields, Arrays.asList(new Tuple(new Object[]{null}), new Tuple(new Object[]{0})));
        resultSet.first();

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        // act 1
        Date r1 = resultSet.getDate("test");

        // assert 1
        assertNull(r1);
        assertTrue(resultSet.wasNull());

        // act 2
        resultSet.next();
        Date r2 = resultSet.getDate("test");

        // assert 2
        assertEquals("1970-01-01", r2.toString());
        assertFalse(resultSet.wasNull());
    }

    @Test
    void testNullableFlagOnTimestampGetter() throws SQLException {
        // arrange
        List<Field> fields = new ArrayList<>();
        Field e = new Field("test", ColumnType.TIMESTAMP);
        fields.add(e);
        prepareResultSet(fields, Arrays.asList(new Tuple(new Object[]{null}), new Tuple(new Object[]{0})));
        resultSet.first();

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        // act 1
        Timestamp r1 = resultSet.getTimestamp("test");

        // assert 1
        assertNull(r1);
        assertTrue(resultSet.wasNull());

        // act 2
        resultSet.next();
        Timestamp r2 = resultSet.getTimestamp("test");

        // assert 2
        assertEquals("1970-01-01 00:00:00.0", r2.toString());
        assertFalse(resultSet.wasNull());
    }

    @Test
    void testNullableFlagOnTimeGetter() throws SQLException {
        // arrange
        List<Field> fields = new ArrayList<>();
        Field e = new Field("test", ColumnType.TIME);
        fields.add(e);
        prepareResultSet(fields, Arrays.asList(new Tuple(new Object[]{null}), new Tuple(new Object[]{0})));
        resultSet.first();

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        // act 1
        Time r1 = resultSet.getTime("test");

        // assert 1
        assertNull(r1);
        assertTrue(resultSet.wasNull());

        // act 2
        resultSet.next();
        Time r2 = resultSet.getTime("test");

        // assert 2
        assertEquals("00:00:00", r2.toString());
        assertFalse(resultSet.wasNull());
    }

    @Test
    void testNullableFlagOnBigDecimalGetter() throws SQLException {
        // arrange
        List<Field> fields = new ArrayList<>();
        Field e = new Field("test", ColumnType.VARCHAR);
        fields.add(e);
        prepareResultSet(fields, Arrays.asList(new Tuple(new Object[]{null}), new Tuple(new Object[]{"1111.1111"})));
        resultSet.first();

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        // act 1
        BigDecimal r1 = resultSet.getBigDecimal("test");

        // assert 1
        assertNull(r1);
        assertTrue(resultSet.wasNull());

        // act 2
        resultSet.next();
        BigDecimal r2 = resultSet.getBigDecimal("test");

        // assert 2
        assertEquals(new BigDecimal("1111.1111"), r2);
        assertFalse(resultSet.wasNull());
    }

    @Test
    void testNullableFlagOnBigDecimalZeroScaleGetter() throws SQLException {
        // arrange
        List<Field> fields = new ArrayList<>();
        Field e = new Field("test", ColumnType.VARCHAR);
        fields.add(e);
        prepareResultSet(fields, Arrays.asList(new Tuple(new Object[]{null}), new Tuple(new Object[]{"1111.1111"})));
        resultSet.first();

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        // act 1
        BigDecimal r1 = resultSet.getBigDecimal("test", 0);

        // assert 1
        assertNull(r1);
        assertTrue(resultSet.wasNull());

        // act 2
        resultSet.next();
        BigDecimal r2 = resultSet.getBigDecimal("test", 0);

        // assert 2
        assertEquals(new BigDecimal("1111"), r2);
        assertFalse(resultSet.wasNull());
    }

    @Test
    void testNullableFlagOnBytesGetter() throws SQLException {
        // arrange
        List<Field> fields = new ArrayList<>();
        Field e = new Field("test", ColumnType.VARCHAR);
        fields.add(e);
        prepareResultSet(fields, Arrays.asList(new Tuple(new Object[]{null}), new Tuple(new Object[]{"1"})));
        resultSet.first();

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        // act 1
        byte[] r1 = resultSet.getBytes("test");

        // assert 1
        assertEquals(0, r1.length);
        assertTrue(resultSet.wasNull());

        // act 2
        resultSet.next();
        byte[] r2 = resultSet.getBytes("test");

        // assert 2
        assertEquals(1, r2.length);
        assertFalse(resultSet.wasNull());
    }

    @Test
    void shouldReturnMetadata() throws SQLException {
        // arrange
        prepareAllTypesResultSet();

        // act
        ResultSetMetaData metaData = resultSet.getMetaData();

        // assert
        assertEquals(ColumnType.values().length, metaData.getColumnCount());
        for (ColumnType type : ColumnType.values()) {
            int index = type.ordinal() + 1;
            assertFalse(metaData.isAutoIncrement(index));
            assertFalse(metaData.isCaseSensitive(index));
            assertTrue(metaData.isSearchable(index));
            assertFalse(metaData.isCurrency(index));
            assertEquals(1, metaData.isNullable(index));
            assertEquals(type == ColumnType.BIGINT || type == ColumnType.INT || type == ColumnType.INT32
                    || type == ColumnType.FLOAT || type == ColumnType.DOUBLE, metaData.isSigned(index));
            assertEquals(80, metaData.getColumnDisplaySize(index));
            assertEquals(type.name(), metaData.getColumnLabel(index));
            assertEquals(type.name(), metaData.getColumnName(index));
            assertEquals("", metaData.getSchemaName(index));
            assertEquals(type == ColumnType.VARCHAR || type == ColumnType.TIMESTAMP ? 6 : 0, metaData.getPrecision(index));
            assertEquals(type == ColumnType.TIMESTAMP ? 6 : 0, metaData.getScale(index));
            assertEquals("", metaData.getTableName(index));
            int columnType = metaData.getColumnType(index);
            if (type == ColumnType.INT) {
                assertEquals(JDBCType.BIGINT.getVendorTypeNumber(), columnType);
            } else if (type == ColumnType.INT32) {
                assertEquals(JDBCType.INTEGER.getVendorTypeNumber(), columnType);
            } else if (type == ColumnType.ANY || type == ColumnType.LINK || type == ColumnType.UUID) {
                assertEquals(JDBCType.VARCHAR.getVendorTypeNumber(), columnType);
            } else {
                assertEquals(JDBCType.valueOf(type.name()).getVendorTypeNumber(), columnType);
            }
            assertEquals(type.name(), metaData.getColumnTypeName(index));
            assertTrue(metaData.isReadOnly(index));
            assertFalse(metaData.isWritable(index));
            assertFalse(metaData.isDefinitelyWritable(index));

            String columnClassName = metaData.getColumnClassName(index);
            switch (type) {
                case VARCHAR:
                case CHAR:
                case UUID:
                case LINK:
                    assertEquals("java.lang.String", columnClassName);
                    break;
                case BIGINT:
                case INT:
                    assertEquals("java.lang.Long", columnClassName);
                    break;
                case INT32:
                    assertEquals("java.lang.Integer", columnClassName);
                    break;
                case DOUBLE:
                    assertEquals("java.lang.Double", columnClassName);
                    break;
                case FLOAT:
                    assertEquals("java.lang.Float", columnClassName);
                    break;
                case DATE:
                    assertEquals("java.sql.Date", columnClassName);
                    break;
                case TIME:
                    assertEquals("java.sql.Time", columnClassName);
                    break;
                case TIMESTAMP:
                    assertEquals("java.sql.Timestamp", columnClassName);
                    break;
                case BOOLEAN:
                    assertEquals("java.lang.Boolean", columnClassName);
                    break;
                case BLOB:
                case ANY:
                    assertEquals("java.lang.Object", columnClassName);
                    break;
            }
        }
    }

    @SneakyThrows
    private void prepareAllTypesResultSet() {
        List<Field> fields = new ArrayList<>();
        List<Object> values = new ArrayList<>();

        for (ColumnType type : ColumnType.values()) {
            Field e = new Field(type.name(), type);

            if (type == ColumnType.VARCHAR || type == ColumnType.TIMESTAMP) {
                e.setSize(6);
            }

            fields.add(e);
            values.add(getObjectByType(type));
        }

        prepareResultSet(fields, Collections.singletonList(new Tuple(values.toArray(new Object[0]))));
        resultSet.first();
    }

    private Object getObjectByType(ColumnType type) {
        switch (type) {
            case INT:
                return 1L;
            case BIGINT:
                return 2L;
            case INT32:
                return 3;
            case VARCHAR:
            case ANY:
            case CHAR:
            case UUID:
            case BLOB:
            case LINK:
                return type.name();
            case FLOAT:
                return 1.1f;
            case DOUBLE:
                return 1.2;
            case BOOLEAN:
                return true;
            case DATE:
                return DateTimeUtils.toEpochDay(LocalDate.parse("2021-01-01"));
            case TIME:
                return DateTimeUtils.toMicros(LocalTime.parse("11:11:11"));
            case TIMESTAMP:
                return DateTimeUtils.toMicros(LocalDateTime.parse("2021-01-01T11:11:11"));
            default:
                throw new AssertionError(String.format("Column type %s is not handled, fix this", type));
        }
    }

    private void prepareResultSet(List<Field> fields, List<Tuple> tuples) {
        Field[] fieldsArray = fields.toArray(new Field[0]);

        when(connection.getTypeInfo()).thenReturn(new TypeInfoCache());

        resultSet = new DtmResultSet(connection, statement, fieldsArray, tuples);
    }
}