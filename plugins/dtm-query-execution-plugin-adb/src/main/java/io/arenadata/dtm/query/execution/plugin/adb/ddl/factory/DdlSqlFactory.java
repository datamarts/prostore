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
package io.arenadata.dtm.query.execution.plugin.adb.ddl.factory;

import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Collections;

import static io.arenadata.dtm.query.execution.plugin.adb.base.factory.Constants.*;

@Service
public class DdlSqlFactory {
    public static final String QUERY_DELIMITER = "; ";

    private static final String DELIMITER = ", ";
    private static final String DROP_TABLE = "DROP TABLE IF EXISTS ";
    private static final String DROP_SCHEMA = "DROP SCHEMA IF EXISTS %s CASCADE";
    private static final String CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS %s";
    private static final String CREATE_INDEX_SQL = "CREATE INDEX %s_%s_%s ON %s.%s_%s (%s)";

    public String createDropTableScript(String tableNameWithSchema) {
        return DROP_TABLE + tableNameWithSchema +
                ACTUAL_TABLE_SUFFIX +
                QUERY_DELIMITER +
                DROP_TABLE + tableNameWithSchema +
                HISTORY_TABLE_SUFFIX +
                QUERY_DELIMITER +
                DROP_TABLE + tableNameWithSchema +
                STAGING_TABLE_SUFFIX +
                QUERY_DELIMITER;
    }

    public String createSchemaSqlQuery(String schema) {
        return String.format(CREATE_SCHEMA, schema);
    }

    public String dropSchemaSqlQuery(String schema) {
        return String.format(DROP_SCHEMA, schema);
    }

    public String createSecondaryIndexSqlQuery(String schema, String table) {
        final String idxPostfix = "_idx";
        return String.format(CREATE_INDEX_SQL, table, ACTUAL_TABLE,
                SYS_FROM_ATTR + idxPostfix, schema, table, ACTUAL_TABLE,
                String.join(DELIMITER, Collections.singletonList(SYS_FROM_ATTR))) +
                QUERY_DELIMITER +
                String.format(CREATE_INDEX_SQL, table, HISTORY_TABLE,
                        SYS_TO_ATTR + idxPostfix, schema, table, HISTORY_TABLE,
                        String.join(DELIMITER, Arrays.asList(SYS_TO_ATTR, SYS_OP_ATTR)));
    }
}
