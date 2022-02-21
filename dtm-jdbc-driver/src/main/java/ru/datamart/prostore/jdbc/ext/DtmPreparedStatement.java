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
import ru.datamart.prostore.common.util.DateTimeUtils;
import ru.datamart.prostore.jdbc.core.*;
import ru.datamart.prostore.jdbc.util.DtmSqlException;
import ru.datamart.prostore.jdbc.util.PreparedStatementParser;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.SQLXML;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.stream.Collectors;

import static java.sql.Types.*;

@Slf4j
public class DtmPreparedStatement extends DtmStatement implements PreparedStatement {
    protected final ParameterList parameters;
    private final String sql;

    public DtmPreparedStatement(BaseConnection c, int rsType, int rsConcurrency, String sql) throws SQLException {
        super(c, rsType, rsConcurrency);
        this.sql = sql;
        this.parameters = new SimpleParameterList(countNonConstantParams(sql));
        super.prepareQuery(sql);
    }

    private int countNonConstantParams(String sql) {
        int count = 0;
        List<List<String>> parameterList = PreparedStatementParser.parse(sql).getParameters();
        for (List<String> pList : parameterList) {
            for (String s : pList) {
                if ("?".equals(s)) {
                    ++count;
                }
            }
        }
        return count;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        execute();
        return result.getResultSet();
    }

    @Override
    public int executeUpdate() throws SQLException {
        return super.executeUpdate(sql);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        parameters.setNull(parameterIndex, sqlType);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean value) throws SQLException {
        parameters.setBoolean(parameterIndex, value, BOOLEAN);
    }

    @Override
    public void setByte(int parameterIndex, byte value) throws SQLException {
        setShort(parameterIndex, value);
    }

    @Override
    public void setShort(int parameterIndex, short value) throws SQLException {
        parameters.setShort(parameterIndex, value, INTEGER);
    }

    @Override
    public void setInt(int parameterIndex, int value) throws SQLException {
        parameters.setInt(parameterIndex, value, INTEGER);
    }

    @Override
    public void setLong(int parameterIndex, long value) throws SQLException {
        parameters.setLong(parameterIndex, value, BIGINT);
    }

    @Override
    public void setFloat(int parameterIndex, float value) throws SQLException {
        parameters.setFloat(parameterIndex, value, FLOAT);
    }

    @Override
    public void setDouble(int parameterIndex, double value) throws SQLException {
        parameters.setDouble(parameterIndex, value, DOUBLE);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal value) throws SQLException {
        parameters.setBigDecimal(parameterIndex, value, DECIMAL);
    }

    @Override
    public void setString(int parameterIndex, String value) throws SQLException {
        parameters.setString(parameterIndex, value, VARCHAR);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] value) throws SQLException {
        parameters.setBytes(parameterIndex, value, ARRAY);
    }

    @Override
    public void setDate(int parameterIndex, Date value) throws SQLException {
        setDate(parameterIndex, value, Calendar.getInstance());
    }

    @Override
    public void setTime(int parameterIndex, Time value) throws SQLException {
        setTime(parameterIndex, value, Calendar.getInstance());
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp value) throws SQLException {
        setTimestamp(parameterIndex, value, Calendar.getInstance());
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream inputStream, int length) throws SQLException {
        String value = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.US_ASCII))
                .lines()
                .collect(Collectors.joining(""))
                .substring(0, length);
        parameters.setString(parameterIndex, value, VARCHAR);
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream inputStream, int length) throws SQLException {
        setAsciiStream(parameterIndex, inputStream, length);
    }

    @Override
    public void clearParameters() throws SQLException {
        parameters.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        this.setObject(parameterIndex, x, targetSqlType, -1);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x == null) {
            this.setNull(parameterIndex, OTHER);
        } else if (x instanceof String) {
            this.setString(parameterIndex, (String) x);
        } else if (x instanceof BigDecimal) {
            this.setBigDecimal(parameterIndex, (BigDecimal) x);
        } else if (x instanceof Short) {
            this.setShort(parameterIndex, (Short) x);
        } else if (x instanceof Integer) {
            this.setInt(parameterIndex, (Integer) x);
        } else if (x instanceof Long) {
            this.setLong(parameterIndex, (Long) x);
        } else if (x instanceof Float) {
            this.setFloat(parameterIndex, (Float) x);
        } else if (x instanceof Double) {
            this.setDouble(parameterIndex, (Double) x);
        } else if (x instanceof byte[]) {
            this.setBytes(parameterIndex, (byte[]) x);
        } else if (x instanceof Date) {
            this.setDate(parameterIndex, (Date) x);
        } else if (x instanceof Time) {
            this.setTime(parameterIndex, (Time) x);
        } else if (x instanceof Timestamp) {
            this.setTimestamp(parameterIndex, (Timestamp) x);
        } else if (x instanceof Boolean) {
            this.setBoolean(parameterIndex, (Boolean) x);
        } else if (x instanceof Byte) {
            this.setByte(parameterIndex, (Byte) x);
        } else if (x instanceof Character) {
            this.setString(parameterIndex, ((Character) x).toString());
        }
    }

    @Override
    public boolean execute() throws SQLException {
        return super.execute(sql, new QueryParameters(parameters.getValues(), parameters.getTypes()));
    }

    @Override
    public void addBatch() throws SQLException {
        checkClosed();
        ArrayList<Query> batchStatements = this.batchStatements;
        if (batchStatements == null) {
            this.batchStatements = batchStatements = new ArrayList<>();
        }
        List<Query> queries = connection.getQueryExecutor().createQuery(sql);
        batchStatements.addAll(queries);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        String value = new BufferedReader(reader)
                .lines()
                .collect(Collectors.joining(""))
                .substring(0, length);
        parameters.setString(parameterIndex, value, VARCHAR);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return result.getResultSet().getMetaData();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        cal.setTime(x);
        LocalDate localDate = LocalDate.of(
                cal.get(Calendar.YEAR),
                cal.get(Calendar.MONTH) + 1,
                cal.get(Calendar.DAY_OF_MONTH));
        long epochDay = DateTimeUtils.toEpochDay(localDate);
        parameters.setDate(parameterIndex, epochDay, DATE);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        cal.setTime(x);
        LocalTime localTime = LocalTime.of(
                cal.get(Calendar.HOUR_OF_DAY),
                cal.get(Calendar.MINUTE),
                cal.get(Calendar.SECOND),
                cal.get(Calendar.MILLISECOND) * 1000_000
        );
        long micros = DateTimeUtils.toMicros(localTime);
        parameters.setTime(parameterIndex, micros, TIME);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp value, Calendar cal) throws SQLException {
        cal.setTime(value);
        LocalDateTime localDateTime = LocalDateTime.of(
                cal.get(Calendar.YEAR),
                cal.get(Calendar.MONTH) + 1,
                cal.get(Calendar.DAY_OF_MONTH),
                cal.get(Calendar.HOUR_OF_DAY),
                cal.get(Calendar.MINUTE),
                cal.get(Calendar.SECOND),
                value.getNanos()
        );
        long micros = DateTimeUtils.toMicros(localDateTime);
        parameters.setTimestamp(parameterIndex, micros, TIMESTAMP);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return createParameterMetaData(connection, parameters.getTypes());
    }

    public ParameterMetaData createParameterMetaData(BaseConnection conn, ColumnType[] paramTypes) throws SQLException {
        return new DtmParameterMetadata(conn, paramTypes);
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(int parameterIndex, Object value, int targetSqlType, int scaleOrLength) throws SQLException {
        if (value == null) {
            parameters.setNull(parameterIndex, targetSqlType);
        } else {
            switch (targetSqlType) {
                //TODO implement bigDecimal setting
                case BOOLEAN:
                    this.setBoolean(parameterIndex, (boolean) value);
                    break;
                case INTEGER:
                    this.setInt(parameterIndex, (int) value);
                    break;
                case FLOAT:
                    this.setFloat(parameterIndex, (float) value);
                    break;
                case DOUBLE:
                    this.setDouble(parameterIndex, (double) value);
                    break;
                case BIGINT:
                    this.setLong(parameterIndex, (long) value);
                    break;
                case CHAR:
                case VARCHAR:
                    this.setString(parameterIndex, value.toString());
                    break;
                case DATE:
                    this.setDate(parameterIndex, (Date) value);
                    break;
                case TIME:
                    this.setTime(parameterIndex, (Time) value);
                    break;
                case TIMESTAMP:
                    this.setTimestamp(parameterIndex, (Timestamp) value);
                    break;
                default:
                    throw new DtmSqlException(String.format("Type %s does not support", targetSqlType));
            }
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setAsciiStream(parameterIndex, x, (int) length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setAsciiStream(parameterIndex, x, (int) length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        setCharacterStream(parameterIndex, reader, (int) length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}
