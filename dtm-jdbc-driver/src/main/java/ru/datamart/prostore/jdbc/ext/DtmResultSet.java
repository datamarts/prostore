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

import lombok.SneakyThrows;
import ru.datamart.prostore.common.configuration.core.CoreConstants;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.jdbc.core.BaseConnection;
import ru.datamart.prostore.jdbc.core.BaseStatement;
import ru.datamart.prostore.jdbc.core.Field;
import ru.datamart.prostore.jdbc.core.Tuple;
import ru.datamart.prostore.jdbc.util.DtmSqlException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;

public class DtmResultSet extends AbstractResultSet {
    private final Field[] fields;
    private final BaseConnection connection;
    private final BaseStatement statement;
    protected List<Tuple> rows;
    protected SQLWarning warnings = null;
    protected boolean wasNullFlag = false;
    protected int fetchSize = 0;
    private int currentRow = -1;
    private Tuple thisRow;
    private ResultSetMetaData rsMetaData;
    private Map<String, Integer> columnNameIndexMap;

    public DtmResultSet(BaseConnection connection, BaseStatement statement, Field[] fields, List<Tuple> tuples) {
        this.connection = connection;
        this.statement = statement;
        this.fields = fields;
        this.rows = tuples;
        this.thisRow = (tuples == null || tuples.isEmpty()) ?
                new Tuple(0) : tuples.get(0);
    }

    public static DtmResultSet createEmptyResultSet() {
        return new DtmResultSet(null,
                null,
                new Field[]{new Field("", ColumnType.VARCHAR)},
                Collections.emptyList());
    }

    @Override
    @SneakyThrows
    public boolean next() {
        if (currentRow + 1 >= rows.size()) {
            return false;
        } else {
            currentRow++;
        }
        initRowBuffer();
        return true;
    }

    @Override
    public boolean first() throws SQLException {
        if (rows.isEmpty()) {
            return false;
        }

        currentRow = 0;
        initRowBuffer();

        return true;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        Object value = getRawValue(columnIndex);
        return value == null ? null : value.toString();
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }


    @SneakyThrows
    @Override
    public int findColumn(String columnLabel) {
        int col = findColumnIndex(columnLabel);
        if (col == 0) {
            throw new DtmSqlException("Column not found: " + columnLabel);
        }
        return col;
    }

    @Override
    public ResultSetMetaData getMetaData() {
        if (rsMetaData == null) {
            rsMetaData = createMetaData();
        }
        return rsMetaData;
    }

    protected ResultSetMetaData createMetaData() {
        return new DtmResultSetMetaData(connection, fields);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        final Field field = fields[columnIndex - 1];
        if (field == null || getRawValue(columnIndex) == null) {
            wasNullFlag = true;
            return null;
        } else {
            switch (field.getDtmType()) {
                case INT:
                case BIGINT:
                    return getLong(columnIndex);
                case INT32:
                    return getInt(columnIndex);
                case VARCHAR:
                case ANY:
                case CHAR:
                case UUID:
                case BLOB:
                case LINK:
                    return getString(columnIndex);
                case FLOAT:
                    return getFloat(columnIndex);
                case DOUBLE:
                    return getDouble(columnIndex);
                case BOOLEAN:
                    return getBoolean(columnIndex);
                case DATE:
                    return getDate(columnIndex);
                case TIME:
                    return getTime(columnIndex);
                case TIMESTAMP:
                    return getTimestamp(columnIndex);
                default:
                    throw new SQLException(String.format("Column type %s for index %s not found!",
                            field.getDtmType(), columnIndex));
            }
        }
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public void close() throws SQLException {
        rows = null;
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        final Object value = getRawValue(columnIndex);
        return value != null && (boolean) value;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        final Object value = getRawValue(columnIndex);
        if (value == null) {
            return 0;
        }

        if (value instanceof Number) {
            return ((Number) value).byteValue();
        }

        throw new ClassCastException("Can't cast to Byte, actual type is: " + value.getClass());
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        final Object value = getRawValue(columnIndex);
        if (value == null) {
            return 0;
        }

        if (value instanceof Number) {
            return ((Number) value).shortValue();
        }

        throw new ClassCastException("Can't cast to Short, actual type is: " + value.getClass());
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        final Object value = getRawValue(columnIndex);
        if (value == null) {
            return 0;
        }

        if (value instanceof Number) {
            return ((Number) value).intValue();
        }

        throw new ClassCastException("Can't cast to Int, actual type is: " + value.getClass());
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        final Object value = getRawValue(columnIndex);
        if (value == null) {
            return 0L;
        }

        if (value instanceof Number) {
            return ((Number) value).longValue();
        }

        throw new ClassCastException("Can't cast to Long, actual type is: " + value.getClass());
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        final Object value = getRawValue(columnIndex);
        if (value == null) {
            return 0f;
        }

        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }

        throw new ClassCastException("Can't cast to Float, actual type is: " + value.getClass());
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Object value = getRawValue(columnIndex);
        if (value == null) {
            return 0D;
        }

        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }

        throw new ClassCastException("Can't cast to Double, actual type is: " + value.getClass());
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        String string = getString(columnIndex);
        if (string == null) {
            return null;
        }

        BigDecimal result = new BigDecimal(string);
        return result.setScale(scale, RoundingMode.HALF_UP);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        final Object value = getRawValue(columnIndex);
        return value == null ? new byte[0] : value.toString().getBytes();
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return getDate(columnIndex, Calendar.getInstance());
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return getTime(columnIndex, Calendar.getInstance());
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return getTimestamp(columnIndex, Calendar.getInstance());
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel), scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel), cal);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        final String value = getString(columnIndex);
        return value == null ? null : new BigDecimal(value);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public int getType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        Object value = getRawValue(columnIndex);
        if (value == null) {
            return null;
        }

        LocalDate localDate = LocalDate.ofEpochDay(((Number) value).longValue());
        cal.set(localDate.getYear(), localDate.getMonthValue() - 1, localDate.getDayOfMonth(),
                0, 0, 0);
        return new Date(cal.getTimeInMillis());
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        Object value = getRawValue(columnIndex);
        if (value == null) {
            return null;
        }

        Instant instant = convertToCalendarInstant(columnIndex, cal, (Number) value);
        return new Time(instant.toEpochMilli());
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        final Object value = getRawValue(columnIndex);
        if (value == null) {
            return null;
        }

        Instant instant = convertToCalendarInstant(columnIndex, cal, (Number) value);
        Timestamp timestamp = new Timestamp(instant.toEpochMilli());
        timestamp.setNanos(instant.getNano());
        return timestamp;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel), cal);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(getClass());
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNullFlag;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return warnings;
    }

    @Override
    public void clearWarnings() throws SQLException {
        warnings = null;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        if (rows >= 0) {
            fetchSize = rows;
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return (rows == null);
    }

    @Override
    public Statement getStatement() throws SQLException {
        return statement;
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        Object value = getRawValue(columnIndex);
        if (value == null) {
            return null;
        }
        String stringValue = getString(columnIndex);
        return new ByteArrayInputStream(stringValue.getBytes(StandardCharsets.US_ASCII));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(findColumn(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        Object value = getRawValue(columnIndex);
        if (value == null) {
            return null;
        }

        return new ByteArrayInputStream(getBytes(columnIndex));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumnIndex(columnLabel));
    }

    public int getRowsSize() {
        return rows.size();
    }

    private void initRowBuffer() {
        thisRow = rows.get(currentRow);
    }

    private int findColumnIndex(String columnName) {
        if (columnNameIndexMap == null) {
            columnNameIndexMap = createColumnNameIndexMap(fields);
        }
        Integer index = columnNameIndexMap.get(columnName);
        if (index != null) {
            return index;
        } else {
            return 0;
        }
    }

    private Map<String, Integer> createColumnNameIndexMap(Field[] fields) {
        Map<String, Integer> indexMap = new HashMap<>(fields.length * 2);

        for (int i = fields.length - 1; i >= 0; --i) {
            String columnLabel = fields[i].getColumnLabel();
            indexMap.put(columnLabel, i + 1);
        }

        return indexMap;
    }

    private Object getRawValue(int columnIndex) throws SQLException {
        if (thisRow == null) {
            throw new DtmSqlException("ResultSet not positioned properly, perhaps you need to call next.");
        }

        Object result = thisRow.get(columnIndex - 1);
        wasNullFlag = result == null;
        return result;
    }

    private Instant convertToCalendarInstant(int columnIndex, Calendar cal, Number value) {
        long timeValue = value.longValue();
        long epochSeconds = timeValue / 1000000L;
        long nanos = getNanos(columnIndex, timeValue);
        long secondsWithCalendarAdjustments = transformSeconds(epochSeconds);
        long millis = adjustMillisByTimezone(secondsWithCalendarAdjustments * 1000, cal);
        return Instant.ofEpochSecond(millis / 1000, nanos);
    }

    private long getNanos(int columnIndex, long tsValue) {
        Field field = fields[columnIndex - 1];
        if (field.getSize() != null) {
            long q = pow(10, 6 - field.getSize());
            return tsValue % 1000000 / q * 1000 * q;
        } else {
            return 0;
        }
    }

    private long pow(long a, long b) {
        int result = 1;
        for (int i = 0; i < b; i++) {
            result *= a;
        }

        return result;
    }

    private long adjustMillisByTimezone(long millis, Calendar cal) {
        if (isSimpleTimeZone(cal.getTimeZone().getID())) {
            return millis - cal.getTimeZone().getRawOffset();
        }

        Calendar transformerCalendar = Calendar.getInstance(CoreConstants.CORE_TIME_ZONE);
        transformerCalendar.setTimeInMillis(millis);
        int era = transformerCalendar.get(Calendar.ERA);
        int year = transformerCalendar.get(Calendar.YEAR);
        int month = transformerCalendar.get(Calendar.MONTH);
        int day = transformerCalendar.get(Calendar.DAY_OF_MONTH);
        int hour = transformerCalendar.get(Calendar.HOUR_OF_DAY);
        int min = transformerCalendar.get(Calendar.MINUTE);
        int sec = transformerCalendar.get(Calendar.SECOND);
        int ms = transformerCalendar.get(Calendar.MILLISECOND);
        cal.set(Calendar.ERA, era);
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.MONTH, month);
        cal.set(Calendar.DAY_OF_MONTH, day);
        cal.set(Calendar.HOUR_OF_DAY, hour);
        cal.set(Calendar.MINUTE, min);
        cal.set(Calendar.SECOND, sec);
        cal.set(Calendar.MILLISECOND, ms);
        return cal.getTimeInMillis();
    }

    private static boolean isSimpleTimeZone(String id) {
        return id.startsWith("GMT") || id.startsWith("UTC");
    }

    private static long transformSeconds(long secs) {
        // Julian/Gregorian calendar cutoff point
        if (secs < -12219292800L) { // October 4, 1582 -> October 15, 1582
            secs += 86400 * 10;
            if (secs < -14825808000L) { // 1500-02-28 -> 1500-03-01
                int extraLeaps = (int) ((secs + 14825808000L) / 3155760000L);
                extraLeaps--;
                extraLeaps -= extraLeaps / 4;
                secs += extraLeaps * 86400L;
            }
        }
        return secs;
    }
}
