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
package ru.datamart.prostore.jdbc.core;

import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.jdbc.util.ColumnTypeUtil;
import ru.datamart.prostore.jdbc.util.DtmSqlException;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Arrays;

public class SimpleParameterList implements ParameterList {

    private Object[] paramValues;
    private ColumnType[] paramTypes;

    public SimpleParameterList(int size) {
        this.paramValues = new Object[size];
        this.paramTypes = new ColumnType[size];
    }

    private void bind(int index, Object value, int sqlType) throws DtmSqlException {
        int normalIndex = index - 1;
        try {
            paramValues[normalIndex] = value;
            paramTypes[normalIndex] = ColumnTypeUtil.getColumnType(sqlType);
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new DtmSqlException(String.format("The column index is out of range: %d, number of columns: %d",
                index, paramValues.length));
        }
    }

    @Override
    public void setNull(int index, int sqlType) throws SQLException {
        this.bind(index, null, sqlType);
    }

    @Override
    public void setBoolean(int index, boolean value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setByte(int index, byte value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setShort(int index, short value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setInt(int index, int value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setLong(int index, long value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setFloat(int index, float value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setDouble(int index, double value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setBigDecimal(int index, BigDecimal value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setString(int index, String value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setBytes(int index, byte[] value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setDate(int index, long value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setTime(int index, long value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public void setTimestamp(int index, long value, int sqlType) throws SQLException {
        this.bind(index, value, sqlType);
    }

    @Override
    public ParameterList copy() {
        SimpleParameterList newCopy = new SimpleParameterList(paramTypes.length);
        newCopy.paramTypes = Arrays.copyOf(paramTypes, paramTypes.length);
        newCopy.paramValues = Arrays.copyOf(paramValues, paramValues.length);
        return newCopy;
    }

    @Override
    public void clear() {
        Arrays.fill(this.paramValues, null);
        Arrays.fill(this.paramTypes, null);
    }

    @Override
    public Object[] getValues() {
        return paramValues;
    }

    @Override
    public ColumnType[] getTypes() {
        return paramTypes;
    }
}
