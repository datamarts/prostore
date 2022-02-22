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

import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.jdbc.core.BaseConnection;
import ru.datamart.prostore.jdbc.core.Field;
import ru.datamart.prostore.jdbc.core.FieldMetadata;
import lombok.SneakyThrows;

import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class DtmResultSetMetaData implements ResultSetMetaData {
    protected final BaseConnection connection;
    protected final Field[] fields;

    public DtmResultSetMetaData(BaseConnection connection, Field[] fields) {
        this.connection = connection;
        this.fields = fields;
    }

    @Override
    public int getColumnCount() {
        return this.fields.length;
    }

    @SneakyThrows
    @Override
    public String getColumnLabel(int column) {
        return this.fields[column - 1].getColumnLabel();
    }

    @Override
    public String getColumnName(int column) {
        return this.fields[column - 1].getColumnLabel();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        FieldMetadata metadata = this.fields[column - 1].getMetadata();
        return metadata == null ? 1 : metadata.getNullable();
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        Field field = this.fields[column - 1];
        ColumnType type = field.getDtmType();
        switch (type) {
            case BIGINT:
            case DOUBLE:
            case FLOAT:
            case INT:
            case INT32:
                return true;
            default:
                return false;
        }
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 80;
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        Field field = this.fields[column - 1];
        Integer size = field.getSize();
        return size == null ? 0 : size;

    }

    @Override
    public int getScale(int column) throws SQLException {
        Field field = this.fields[column - 1];
        switch (field.getDtmType()) {
            case TIME:
            case TIMESTAMP:
                Integer size = field.getSize();
                return size == null ? 0 : size;
            default:
                return 0;
        }
    }

    @Override
    public String getTableName(int column) throws SQLException {
        FieldMetadata metadata = this.fields[column - 1].getMetadata();
        return metadata == null ? "" : metadata.getTableName();
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        ColumnType type = this.fields[column - 1].getDtmType();
        if (type == ColumnType.INT32) {
            return JDBCType.INTEGER.getVendorTypeNumber();
        }
        if (type == ColumnType.INT) {
            return JDBCType.BIGINT.getVendorTypeNumber();
        }
        if (type == ColumnType.ANY || type == ColumnType.LINK || type == ColumnType.UUID) {
            return JDBCType.VARCHAR.getVendorTypeNumber();
        } else {
            return JDBCType
                    .valueOf(type.name())
                    .getVendorTypeNumber();
        }
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return this.fields[column - 1].getDtmType().name();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return connection.getTypeInfo().getJavaClass(this.fields[column - 1].getDtmType());
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
}
