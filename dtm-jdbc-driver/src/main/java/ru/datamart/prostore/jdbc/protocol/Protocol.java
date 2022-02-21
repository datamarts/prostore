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
package ru.datamart.prostore.jdbc.protocol;

import ru.datamart.prostore.jdbc.core.QueryRequest;
import ru.datamart.prostore.jdbc.core.QueryResult;
import ru.datamart.prostore.jdbc.model.ColumnInfo;
import ru.datamart.prostore.jdbc.model.SchemaInfo;
import ru.datamart.prostore.jdbc.model.TableInfo;

import java.sql.SQLException;
import java.util.List;

/**
 * Data reader protocol
 */
public interface Protocol {
    /**
     * Get schemas information
     * @return List of schema info
     */
    List<SchemaInfo> getDatabaseSchemas();
    /**
     * Get table information for schema pattern
     * @param schemaPattern - schema pattern
     * @return List of table info
     */
    List<TableInfo> getDatabaseTables(String schemaPattern);
    /**
     * Get column info for schema, table
     * @param schema - schema name
     * @param tableName - table name
     * @return List of column info
     */
    List<ColumnInfo> getDatabaseColumns(String schema, String tableName);
    /**
     * execute sql query without params
     * @param request query request
     * @return query result
     */
    QueryResult executeQuery(QueryRequest request) throws SQLException;
}
