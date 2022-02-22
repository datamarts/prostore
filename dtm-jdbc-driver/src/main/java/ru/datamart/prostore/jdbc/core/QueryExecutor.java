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

import ru.datamart.prostore.jdbc.model.ColumnInfo;
import ru.datamart.prostore.jdbc.model.SchemaInfo;
import ru.datamart.prostore.jdbc.model.TableInfo;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;

public interface QueryExecutor {

    void execute(Query query, QueryParameters parameters, ResultHandler resultHandler);

    void execute(List<Query> queries, List<QueryParameters> parametersList, ResultHandler resultHandler);

    List<Query> createQuery(String sql) throws SQLException;

    List<SchemaInfo> getSchemas();

    List<TableInfo> getTables(String schema);

    List<ColumnInfo> getTableColumns(String schema, String table);

    String getUser();

    String getDatabase();

    void setDatabase(String schema);

    String getServerVersion();

    String getUrl();

    SQLWarning getWarnings();

    boolean isClosed();

    void close();
}
