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

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import static ru.datamart.prostore.common.model.ddl.ColumnType.*;

public class TypeInfoCache implements TypeInfo {

    private final BaseConnection conn;
    private final Map<ColumnType, String> dtmTypeToJavaClassMap;
    private final Map<ColumnType, Integer> dtmTypeToSqlTypeMap;
    private final Map<ColumnType, String> dtmTypeToAliasTypeMap;
    private static final Object[][] types = new Object[][]{
            {VARCHAR, Types.VARCHAR, "java.lang.String", "varchar"},
            {CHAR, Types.CHAR, "java.lang.String", "char"},
            {BIGINT, Types.BIGINT, "java.lang.Long", "bigint"},
            {INT, Types.BIGINT, "java.lang.Long", "bigint"},
            {INT32, Types.INTEGER, "java.lang.Integer", "int32"},
            {DOUBLE, Types.DOUBLE, "java.lang.Double", "double"},
            {FLOAT, Types.FLOAT, "java.lang.Float", "float"},
            {DATE, Types.DATE, "java.sql.Date", "date"},
            {TIME, Types.TIME, "java.sql.Time", "time"},
            {TIMESTAMP, Types.TIMESTAMP, "java.sql.Timestamp", "timestamp"},
            {BOOLEAN, Types.BOOLEAN, "java.lang.Boolean", "boolean"},
            {BLOB, Types.BLOB, "java.lang.Object", "blob"},
            {UUID, Types.OTHER, "java.lang.String", "uuid"},
            {LINK, Types.VARCHAR, "java.lang.String", "link"},
            {ANY, Types.OTHER, "java.lang.Object", "any"}
    };

    public TypeInfoCache(BaseConnection conn) {
        this.conn = conn;
        this.dtmTypeToSqlTypeMap = new HashMap((int) Math.round((double) types.length * 1.5D));
        this.dtmTypeToAliasTypeMap = new HashMap((int) Math.round((double) types.length * 1.5D));
        this.dtmTypeToJavaClassMap = new HashMap((int) Math.round((double) types.length * 1.5D));
        for (Object[] type : types) {
            ColumnType dtmType = (ColumnType) type[0];
            Integer sqlType = (Integer) type[1];
            String javaClass = (String) type[2];
            String dtmTypeName = (String) type[3];
            this.addCoreType(dtmType, sqlType, javaClass, dtmTypeName);
        }

    }

    private void addCoreType(ColumnType dtmType, Integer sqlType, String javaClass, String dtmTypeName) {
        this.dtmTypeToAliasTypeMap.put(dtmType, dtmTypeName);
        this.dtmTypeToJavaClassMap.put(dtmType, javaClass);
        this.dtmTypeToSqlTypeMap.put(dtmType, sqlType);
    }

    @Override
    public boolean isSigned(ColumnType type) {
        switch (type) {
            case BIGINT:
            case DOUBLE:
            case FLOAT:
            case INT:
                return true;
            default:
                return false;
        }
    }

    @Override
    public String getJavaClass(ColumnType type) {
        return dtmTypeToJavaClassMap.get(type);
    }

    @Override
    public Integer getSqlType(ColumnType type) {
        return dtmTypeToSqlTypeMap.get(type);
    }

    @Override
    public String getAlias(ColumnType type) {
        return dtmTypeToAliasTypeMap.get(type);
    }
}
