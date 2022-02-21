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
package ru.datamart.prostore.jdbc.util;

public class DriverConstants {
    public static final String CONNECT_URL_PREFIX = "jdbc:prostore://";

    //Connection parameters
    public static final String SCHEMA_PROPERTY = "schema";
    public static final String HOST_PROPERTY = "dbHost";
    public static final String USER_PROPERTY = "user";

    //System columns names
    public static final String CATALOG_NAME_COLUMN = "TABLE_CAT";
    public static final String SCHEMA_NAME_COLUMN = "TABLE_SCHEM";
    public static final String TABLE_NAME_COLUMN = "TABLE_NAME";
    public static final String COLUMN_NAME_COLUMN = "COLUMN_NAME";
    public static final String COLUMN_SIZE_COLUMN = "COLUMN_SIZE";
    public static final String COLUMN_DEF_COLUMN = "COLUMN_DEF";
    public static final String BUFFER_LENGTH_COLUMN = "BUFFER_LENGTH";
    public static final String DECIMAL_DIGITS_COLUMN = "DECIMAL_DIGITS";
    public static final String NULLABLE_COLUMN = "NULLABLE";
    public static final String IS_NULLABLE_COLUMN = "IS_NULLABLE";
    public static final String TABLE_TYPE_COLUMN = "TABLE_TYPE";
    public static final String DATA_TYPE_COLUMN = "DATA_TYPE";
    public static final String TYPE_NAME_COLUMN = "TYPE_NAME";
    public static final String PRECISION_COLUMN = "PRECISION";
    public static final String REMARKS_COLUMN = "REMARKS";
    public static final String SELF_REFERENCING_COL_NAME_COLUMN = "SELF_REFERENCING_COL_NAME";
    public static final String REF_GENERATION_COLUMN = "REF_GENERATION";
    public static final String TABLE_OWNER_COLUMN = "TABLE_OWNER";
    public static final String NUM_PREC_RADIX_COLUMN = "NUM_PREC_RADIX";
    public static final String SQL_DATA_TYPE_COLUMN = "SQL_DATA_TYPE";
    public static final String SQL_DATETIME_SUB_COLUMN = "SQL_DATETIME_SUB";
    public static final String CHAR_OCTET_LENGTH_COLUMN = "CHAR_OCTET_LENGTH";
    public static final String ORDINAL_POSITION_COLUMN = "ORDINAL_POSITION";
    public static final String SCOPE_CATALOG_COLUMN = "SCOPE_CATALOG";
    public static final String SCOPE_SCHEMA_COLUMN = "SCOPE_SCHEMA";
    public static final String SCOPE_TABLE_COLUMN = "SCOPE_TABLE";
    public static final String SOURCE_DATA_TYPE_COLUMN = "SOURCE_DATA_TYPE";
    public static final String IS_AUTOINCREMENT_COLUMN = "IS_AUTOINCREMENT";
    public static final String IS_GENERATEDCOLUMN_COLUMN = "IS_GENERATEDCOLUMN";
    public static final String SPECIFIC_NAME_COLUMN = "SPECIFIC_NAME";

    public static final String TABLE_TYPE = "TABLE";
    public static final String SYSTEM_VIEW_TYPE = "SYSTEM VIEW";
    public static final String VIEW_TYPE = "VIEW";

    private DriverConstants() {
    }
}
