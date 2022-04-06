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
package ru.datamart.prostore.jdbc.core;

import ru.datamart.prostore.common.model.ddl.ColumnType;

import java.sql.Types;
import java.util.EnumMap;
import java.util.Map;

import static ru.datamart.prostore.common.model.ddl.ColumnType.*;

public class TypeInfoCache implements TypeInfo {

    private static final String JAVA_LANG_STRING = "java.lang.String";
    private static final Map<ColumnType, String> dtmTypeToJavaClassMap = new EnumMap<>(ColumnType.class);
    private static final Map<ColumnType, Integer> dtmTypeToSqlTypeMap = new EnumMap<>(ColumnType.class);
    private static final Map<ColumnType, String> dtmTypeToAliasTypeMap = new EnumMap<>(ColumnType.class);
    private static final Object[][] types = new Object[][]{
            {VARCHAR, Types.VARCHAR, JAVA_LANG_STRING, "varchar"},
            {CHAR, Types.CHAR, JAVA_LANG_STRING, "char"},
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
            {UUID, Types.OTHER, JAVA_LANG_STRING, "uuid"},
            {LINK, Types.VARCHAR, JAVA_LANG_STRING, "link"},
            {ANY, Types.OTHER, "java.lang.Object", "any"}
    };

    static {
        for (Object[] type : types) {
            ColumnType dtmType = (ColumnType) type[0];
            Integer sqlType = (Integer) type[1];
            String javaClass = (String) type[2];
            String dtmTypeName = (String) type[3];
            dtmTypeToAliasTypeMap.put(dtmType, dtmTypeName);
            dtmTypeToJavaClassMap.put(dtmType, javaClass);
            dtmTypeToSqlTypeMap.put(dtmType, sqlType);
        }
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
