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
package ru.datamart.prostore.query.execution.core.base.utils;

public class InformationSchemaUtils {

    private InformationSchemaUtils() {
    }

    public static final String INFORMATION_SCHEMA = "information_schema";

    public static final String LOGIC_SCHEMA_KEY_COLUMN_USAGE =
        "CREATE VIEW IF NOT EXISTS DTM.logic_schema_key_column_usage AS \n" +
                "select sii.TABLE_CAT        as constraint_catalog,\n" +
                "       sii.TABLE_SCHEM      as constraint_schema,\n" +
                "       case\n" +
                "           when sii.index_name like 'SYS_IDX_PK_%' then siu.constraint_name\n" +
                "           else 'SK_' + sii.TABLE_SCHEM + '_' + sii.TABLE_NAME\n" +
                "           end              as constraint_name,\n" +
                "       sii.table_schem      as table_schema,\n" +
                "       sii.table_name       as table_name,\n" +
                "       sii.column_name      as column_name,\n" +
                "       sii.ordinal_position as ordinal_position\n" +
                "FROM information_schema.SYSTEM_INDEXINFO sii\n" +
                "         left join information_schema.SYSTEM_KEY_INDEX_USAGE siu\n" +
                "                   on sii.TABLE_SCHEM = siu.INDEX_SCHEMA\n" +
                "                       and sii.TABLE_SCHEM = siu.CONSTRAINT_SCHEMA\n" +
                "                       and sii.TABLE_CAT = siu.CONSTRAINT_CATALOG\n" +
                "                       and sii.INDEX_NAME = siu.INDEX_NAME\n" +
                "WHERE sii.TABLE_SCHEM NOT IN ('DTM', 'INFORMATION_SCHEMA', 'SYSTEM_LOBS');";

    public static final String LOGIC_SCHEMA_DATAMARTS =
        "CREATE VIEW IF NOT EXISTS DTM.logic_schema_datamarts AS \n" +
            "SELECT catalog_name, schema_name\n" +
            "FROM information_schema.schemata \n" +
            "WHERE schema_name NOT IN ('DTM', 'INFORMATION_SCHEMA', 'SYSTEM_LOBS')";

    public static final String LOGIC_SCHEMA_ENTITIES =
        "CREATE VIEW IF NOT EXISTS DTM.logic_schema_entities AS\n" +
                "SELECT table_catalog, \n" +
                "       table_schema, \n" +
                "       table_name, \n" +
                "       regexp_substring_array(com.comment, '^[^;]*|[^;]+')[2] as table_type,\n" +
                "       regexp_substring_array(com.comment, '^[^;]*|[^;]+')[1] as table_datasource_type\n" +
                "FROM information_schema.tables t\n" +
                "         left outer join information_schema.system_comments com\n" +
                "                         on t.TABLE_CATALOG = com.OBJECT_CATALOG\n" +
                "                             and t.TABLE_SCHEMA = com.OBJECT_SCHEMA\n" +
                "                             and t.TABLE_NAME = com.OBJECT_NAME\n" +
                "                             and com.COLUMN_NAME IS NULL\n" +
                "WHERE table_schema NOT IN ('DTM', 'INFORMATION_SCHEMA', 'SYSTEM_LOBS')" +
                "  AND t.table_name NOT LIKE 'SYS_%'";

    public static final String LOGIC_SCHEMA_COLUMNS =
        "CREATE VIEW IF NOT EXISTS DTM.logic_schema_columns AS\n" +
                "SELECT c.table_catalog,\n" +
                "       c.table_schema,\n" +
                "       c.table_name,\n" +
                "       c.column_name,\n" +
                "       c.is_nullable,\n" +
                "       c.ordinal_position,\n" +
                "       case c.character_maximum_length" +
                "           when 16777216 then NULL " +
                "           else c.character_maximum_length " +
                "           end as character_maximum_length,\n" +
                "       case when c.datetime_precision != 0 then c.datetime_precision " +
                "       else NULL end as datetime_precision,\n" +
                "       case\n" +
                "           when com.comment is not NULL then comment\n" +
                "           else c.data_type\n" +
                "           end as data_type\n" +
                "FROM information_schema.COLUMNS c\n" +
                "         left outer join information_schema.system_comments com\n" +
                "                         on c.TABLE_CATALOG = com.OBJECT_CATALOG and c.TABLE_SCHEMA = com.OBJECT_SCHEMA and\n" +
                "                            c.TABLE_NAME = com.OBJECT_NAME and\n" +
            "                            c.COLUMN_NAME = com.COLUMN_NAME\n" +
            "WHERE c.table_schema NOT IN ('DTM', 'INFORMATION_SCHEMA', 'SYSTEM_LOBS')";

    public static final String LOGIC_SCHEMA_ENTITY_CONSTRAINTS =
        "CREATE VIEW IF NOT EXISTS DTM.logic_schema_entity_constraints AS\n" +
                "SELECT si.TABLE_CATALOG as constraint_catalog,\n" +
                "       si.TABLE_SCHEMA as constraint_schema,\n" +
                "       case when si.index_name like 'SYS_IDX_PK_%' then siu.constraint_name\n" +
                "            else 'SK_' + si.TABLE_SCHEMA + '_' + si.TABLE_NAME\n" +
                "            end as constraint_name,\n" +
                "       si.table_schema,\n" +
                "       si.table_name,\n" +
                "       case\n" +
                "           when si.INDEX_NAME like 'SK_%' then 'sharding key'\n" +
                "           when si.INDEX_NAME like 'SYS_IDX_PK_%' then cast('primary key' AS VARCHAR(12))\n" +
                "           else cast('-' AS VARCHAR(1))\n" +
                "           end       as constraint_type\n" +
                "FROM information_schema.SYSTEM_INDEXSTATS si\n" +
                "     left join information_schema.SYSTEM_KEY_INDEX_USAGE siu\n" +
                "          on siu.CONSTRAINT_CATALOG = si.TABLE_CATALOG\n" +
                "              and siu.INDEX_CATALOG = si.TABLE_CATALOG\n" +
                "              and siu.CONSTRAINT_SCHEMA = si.TABLE_SCHEMA\n" +
                "              and siu.INDEX_NAME = si.INDEX_NAME\n" +
                "WHERE   si.TABLE_SCHEMA NOT IN ('DTM', 'INFORMATION_SCHEMA', 'SYSTEM_LOBS');";

    public static final String CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS %s";
    public static final String SELECT_VIEWS_BY_SCHEMA = "SELECT table_name FROM information_schema.tables\n " +
            "WHERE table_schema = '%s' AND table_type = 'VIEW'";
    public static final String DROP_SCHEMA = "DROP SCHEMA %s CASCADE";

    public static final String CREATE_SHARDING_KEY_INDEX = "CREATE INDEX IF NOT EXISTS sk_%s on %s (%s)";

    public static final String COMMENT_ON_COLUMN = "COMMENT ON COLUMN %s.%s IS '%s'";
    public static final String COMMENT_ON_TABLE = "COMMENT ON TABLE %s IS '%s'";

    public static final String DROP_VIEW = "DROP VIEW IF EXISTS %s.%s";
    public static final String DROP_TABLE = "DROP TABLE IF EXISTS %s.%s";
    public static final String CHECK_VIEW_BY_TABLE_NAME =
            "SELECT VIEW_NAME\n" +
            "FROM   INFORMATION_SCHEMA.VIEW_TABLE_USAGE\n" +
            "WHERE  TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s';";
    public static final String CHECK_VIEW_BY_TABLE_SCHEMA =
            "SELECT VIEW_SCHEMA, VIEW_NAME\n" +
                    "FROM   INFORMATION_SCHEMA.VIEW_TABLE_USAGE\n" +
                    "WHERE  TABLE_SCHEMA = '%s' AND VIEW_SCHEMA != '%s';";
}
