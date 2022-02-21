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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.util.List;

public interface ParameterList {

    void setNull(int index, int sqlType) throws SQLException;

    void setBoolean(int index, boolean value, int sqlType) throws SQLException;

    void setByte(int index, byte value, int sqlType) throws SQLException;

    void setShort(int index, short value, int sqlType) throws SQLException;

    void setInt(int index, int value, int sqlType) throws SQLException;

    void setLong(int index, long value, int sqlType) throws SQLException;

    void setFloat(int index, float value, int sqlType) throws SQLException;

    void setDouble(int index, double value, int sqlType) throws SQLException;

    void setBigDecimal(int index, BigDecimal value, int sqlType) throws SQLException;

    void setString(int index, String value, int sqlType) throws SQLException;

    void setBytes(int index, byte[] value, int sqlType) throws SQLException;

    void setDate(int index, long value, int sqlType) throws SQLException;

    void setTime(int index, long value, int sqlType) throws SQLException;

    void setTimestamp(int index, long value, int sqlType) throws SQLException;

    ParameterList copy();

    void clear();

    Object[] getValues();

    ColumnType[] getTypes();
}
