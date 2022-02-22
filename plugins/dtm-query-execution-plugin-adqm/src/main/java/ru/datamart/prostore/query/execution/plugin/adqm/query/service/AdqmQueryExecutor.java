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
package ru.datamart.prostore.query.execution.plugin.adqm.query.service;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import lombok.extern.slf4j.Slf4j;
import ru.datamart.prostore.async.AsyncUtils;
import ru.datamart.prostore.common.converter.SqlTypeConverter;
import ru.datamart.prostore.common.reader.QueryParameters;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import ru.datamart.prostore.query.execution.plugin.api.exception.DataSourceException;

import javax.sql.DataSource;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class AdqmQueryExecutor implements DatabaseExecutor {
    private final SQLClient sqlClient;
    private final SqlTypeConverter fromAdqmSqlTypeConverter;
    private final SqlTypeConverter toAdqmSqlTypeConverter;

    public AdqmQueryExecutor(Vertx vertx,
                             DataSource adqmDataSource,
                             SqlTypeConverter fromAdqmSqlTypeConverter,
                             SqlTypeConverter toAdqmSqlTypeConverter) {
        this.fromAdqmSqlTypeConverter = fromAdqmSqlTypeConverter;
        this.toAdqmSqlTypeConverter = toAdqmSqlTypeConverter;
        this.sqlClient = JDBCClient.create(vertx, adqmDataSource);
    }

    @Override
    public Future<List<Map<String, Object>>> execute(String sql, List<ColumnMetadata> metadata) {
        log.debug("ADQM. Execute query: [{}] ", sql);
        //TODO perhaps it's better to use RowStream interface for getting rows one by one and create chunks here
        return AsyncUtils.measureMs(getSqlConnection()
                        .compose(conn -> executeQuery(conn, sql)),
                duration -> log.debug("ADQM. Query completed successfully: [{}] in [{}]ms", sql, duration))
                .map(resultSet -> {
                    try {
                        return createResult(metadata, resultSet);
                    } catch (Exception e) {
                        throw new DataSourceException("Error converting value to jdbc type", e);
                    }
                });
    }

    @Override
    public Future<Void> executeUpdate(String sql) {
        log.debug("ADQM. Execute update: [{}] ", sql);
        return AsyncUtils.measureMs(getSqlConnection()
                        .compose(conn -> executeQueryUpdate(conn, sql)),
                duration -> log.debug("ADQM. Update completed successfully: [{}] in [{}]ms", sql, duration))
                .onFailure(err -> log.error(err.getMessage()));
    }

    @Override
    public Future<List<Map<String, Object>>> executeWithParams(String sql,
                                                               QueryParameters params,
                                                               List<ColumnMetadata> metadata) {
        log.debug("ADQM. Execute query: [{}] with params: [{}]", sql, params);
        //TODO perhaps it's better to use RowStream interface for getting rows one by one and create chunks here
        return AsyncUtils.measureMs(getSqlConnection()
                        .compose(conn -> executeQueryWithParams(conn, sql, createParamsArray(params))),
                duration -> log.debug("ADQM. Query completed successfully: [{}] in [{}]ms", sql, duration))
                .map(resultSet -> {
                    try {
                        return createResult(metadata, resultSet);
                    } catch (Exception e) {
                        throw new DataSourceException("Error converting value to jdbc type", e);
                    }
                });
    }

    private JsonArray createParamsArray(QueryParameters params) {
        if (params == null) {
            return new JsonArray(Collections.emptyList());
        } else {
            List<Object> values = IntStream.range(0, params.getValues().size())
                    .mapToObj(n -> toAdqmSqlTypeConverter.convert(params.getTypes().get(n),
                            params.getValues().get(n)))
                    .collect(Collectors.toList());
            return new JsonArray(values);
        }
    }

    private Future<SQLConnection> getSqlConnection() {
        return Future.future(sqlClient::getConnection);
    }

    private Future<ResultSet> executeQuery(SQLConnection conn, String sql) {
        return Future.future(promise -> conn.query(sql, promise));
    }

    private Future<ResultSet> executeQueryWithParams(SQLConnection conn, String sql, JsonArray params) {
        return Future.future(promise -> conn.queryWithParams(sql, params, promise));
    }

    private Future<Void> executeQueryUpdate(SQLConnection conn, String sql) {
        return Future.future(promise -> conn.execute(sql, promise));
    }

    private List<Map<String, Object>> createResult(List<ColumnMetadata> metadata, ResultSet rs) {
        Map<String, Integer> columnIndexMap = new HashMap<>();
        Function<JsonObject, Map<String, Object>> func = metadata.isEmpty()
                ? JsonObject::getMap
                : row -> createRowMap(metadata, columnIndexMap, row);
        return Optional.ofNullable(rs)
                .map(resultSet -> resultSet.getRows().stream()
                        .map(func)
                        .collect(Collectors.toList()))
                .orElse(Collections.emptyList());
    }

    private void initColumnIndexMap(Map<String, Integer> columnIndexMap, JsonObject row) {
        final List<String> fields = new ArrayList<>(row.fieldNames());
        columnIndexMap.putAll(IntStream.range(0, fields.size())
                .boxed()
                .collect(Collectors.toMap(fields::get, i -> i)));
    }

    private Map<String, Object> createRowMap(List<ColumnMetadata> metadata, Map<String, Integer> columnIndexMap,
                                             JsonObject row) {
        if (columnIndexMap.isEmpty()) {
            initColumnIndexMap(columnIndexMap, row);
        }
        Map<String, Object> rowMap = new HashMap<>();
        row.stream().forEach(column -> {
            final ColumnMetadata columnMetadata = metadata.get(columnIndexMap.get(column.getKey()));
            rowMap.put(columnMetadata.getName(), fromAdqmSqlTypeConverter.convert(columnMetadata.getType(), column.getValue()));
        });
        return rowMap;
    }
}
