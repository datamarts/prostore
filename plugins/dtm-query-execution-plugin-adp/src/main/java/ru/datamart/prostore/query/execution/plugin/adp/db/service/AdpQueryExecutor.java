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
package ru.datamart.prostore.query.execution.plugin.adp.db.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.*;
import io.vertx.sqlclient.impl.ArrayTuple;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import ru.datamart.prostore.async.AsyncUtils;
import ru.datamart.prostore.common.converter.SqlTypeConverter;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.plugin.sql.PreparedStatementRequest;
import ru.datamart.prostore.common.reader.QueryParameters;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import ru.datamart.prostore.query.execution.plugin.api.exception.LlrDatasourceException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class AdpQueryExecutor implements DatabaseExecutor {

    private final PgPool pool;
    private final int fetchSize;
    private final SqlTypeConverter fromAdpSqlTypeConverter;
    private final SqlTypeConverter toAdpSqlTypeConverter;

    public AdpQueryExecutor(PgPool pool,
                            int fetchSize,
                            SqlTypeConverter fromAdpSqlTypeConverter,
                            SqlTypeConverter toAdpSqlTypeConverter) {
        this.pool = pool;
        this.fetchSize = fetchSize;
        this.fromAdpSqlTypeConverter = fromAdpSqlTypeConverter;
        this.toAdpSqlTypeConverter = toAdpSqlTypeConverter;
    }

    @Override
    public Future<List<Map<String, Object>>> execute(String sql, List<ColumnMetadata> metadata) {
        return executeWithParams(sql, null, metadata);
    }

    @Override
    public Future<List<Map<String, Object>>> executeWithCursor(String sql, List<ColumnMetadata> metadata) {
        return Future.future(promise -> {
            log.debug("ADP. Execute with cursor: [{}]", sql);
            pool.withConnection(conn -> AsyncUtils.measureMs(prepareQuery(conn, sql)
                            .compose(pgPreparedQuery -> readDataWithCursor(pgPreparedQuery, metadata, fetchSize)),
                    duration -> log.debug("ADP. Execute with cursor succeeded: [{}] in [{}]ms", sql, duration)))
                    .onSuccess(promise::complete)
                    .onFailure(e -> {
                        log.error("ADP. Execute with cursor failed: [{}]", sql, e);
                        promise.fail(e);
                    });
        });
    }

    @Override
    public Future<List<Map<String, Object>>> executeWithParams(String sql,
                                                               QueryParameters params,
                                                               List<ColumnMetadata> metadata) {
        return Future.future(promise -> {
            log.debug("ADP. Execute query: [{}] with params: [{}]", sql, params);
            pool.withConnection(conn -> AsyncUtils.measureMs(executePreparedQuery(conn, sql, createParamsArray(params)),
                    duration -> log.debug("ADP. Execute with params succeeded: [{}] in [{}]ms", sql, duration))
                    .map(rowSet -> createResult(metadata, rowSet)))
                    .onSuccess(promise::complete)
                    .onFailure(fail -> {
                        log.error("ADP. Execute with params failed: [{}]", sql, fail);
                        promise.fail(fail);
                    });
        });
    }

    private ArrayTuple createParamsArray(QueryParameters params) {
        if (params == null || params.getValues().isEmpty()) {
            return null;
        }

        return new ArrayTuple(IntStream.range(0, params.getValues().size())
                .mapToObj(n -> toAdpSqlTypeConverter.convert(params.getTypes().get(n),
                        params.getValues().get(n)))
                .collect(Collectors.toList()));
    }

    @Override
    public Future<Void> executeUpdate(String sql) {
        return Future.future(promise -> {
            log.debug("ADP. Execute update: [{}]", sql);
            pool.withConnection(conn -> AsyncUtils.measureMs(executeQueryUpdate(conn, sql),
                    duration -> log.debug("ADP. Execute update succeeded: [{}] in [{}]ms", sql, duration)))
                    .onSuccess(result -> promise.complete())
                    .onFailure(fail -> {
                        log.error("ADP. Execute update failed: [{}]", sql, fail);
                        promise.fail(fail);
                    });
        });
    }

    private Future<List<Map<String, Object>>> readDataWithCursor(PreparedStatement preparedQuery,
                                                                 List<ColumnMetadata> metadata,
                                                                 Integer fetchSize) {
        return Future.future(promise -> {
            List<Map<String, Object>> result = new ArrayList<>();
            final Cursor pgCursor = preparedQuery.cursor();
            readCursor(pgCursor, fetchSize, metadata, ar -> {
                        if (ar.succeeded()) {
                            result.addAll(ar.result());
                        } else {
                            promise.fail(ar.cause());
                        }
                    },
                    rr -> {
                        if (rr.succeeded()) {
                            promise.complete(result);
                        } else {
                            promise.fail(new DtmException("Error executing fetching data with cursor", rr.cause()));
                        }
                    });
        });
    }

    private void readCursor(Cursor cursor,
                            int chunkSize,
                            List<ColumnMetadata> metadata,
                            Handler<AsyncResult<List<Map<String, Object>>>> itemHandler,
                            Handler<AsyncResult<List<Map<String, Object>>>> handler) {
        cursor.read(chunkSize, res -> {
            if (res.succeeded()) {
                val dataSet = createResult(metadata, res.result());
                itemHandler.handle(Future.succeededFuture(dataSet));
                if (cursor.hasMore()) {
                    readCursor(cursor,
                            chunkSize,
                            metadata,
                            itemHandler,
                            handler);
                } else {
                    cursor.close();
                    handler.handle(Future.succeededFuture(dataSet));
                }
            } else {
                handler.handle(Future.failedFuture(res.cause()));
            }
        });
    }

    @Override
    public Future<Void> executeInTransaction(List<PreparedStatementRequest> requests) {
        return Future.future(p -> {
            log.debug("ADP. Execute in transaction: {}", requests);
            AsyncUtils.measureMs(pool.withTransaction(connection -> {
                        Future<Void> lastFuture = Future.succeededFuture();
                        for (PreparedStatementRequest st : requests) {
                            log.debug("ADP. Execute query in transaction: [{}] with params: [{}]", st.getSql(), st.getParams());
                            lastFuture = lastFuture.compose(s -> AsyncUtils.measureMs(execute(st, connection),
                                    duration -> log.debug("ADP. Execute query in transaction succeeded: [{}] in [{}]ms", st.getSql(), duration)));
                        }
                        return lastFuture;
                    }),
                    duration -> log.debug("ADP. Execute in transaction sucess: [{}] in [{}]ms", requests, duration))
                    .onSuccess(event -> p.complete())
                    .onFailure(err -> {
                        log.error("ADP. Execute in transaction failed: [{}]", requests, err);
                        p.fail(new LlrDatasourceException(String.format("Error executing queries: %s",
                                err.getMessage())));
                    });
        });
    }

    private Future<PreparedStatement> prepareQuery(SqlConnection conn, String sql) {
        return Future.future(promise -> conn.prepare(sql, promise));
    }

    private Future<RowSet<Row>> executeQueryUpdate(SqlConnection conn, String sql) {
        return Future.future(promise -> conn.query(sql).execute(promise));
    }

    private Future<RowSet<Row>> executePreparedQuery(SqlConnection conn, String sql, Tuple params) {
        return Future.future(promise -> {
            if (params == null) {
                conn.query(sql).execute(promise);
                return;
            }

            conn.preparedQuery(sql).execute(params, promise);
        });
    }

    private List<Map<String, Object>> createResult(List<ColumnMetadata> metadata,
                                                   RowSet<Row> pgRowSet) {
        List<Map<String, Object>> result = new ArrayList<>();
        Function<Row, Map<String, Object>> func = metadata.isEmpty()
                ? row -> createRowMap(row, pgRowSet.columnsNames().size())
                : row -> createRowMap(metadata, row);
        for (Row row : pgRowSet) {
            result.add(func.apply(row));
        }
        return result;
    }

    private Map<String, Object> createRowMap(List<ColumnMetadata> metadata, Row row) {
        Map<String, Object> rowMap = new HashMap<>();
        for (int i = 0; i < metadata.size(); i++) {
            ColumnMetadata columnMetadata = metadata.get(i);
            rowMap.put(columnMetadata.getName(),
                    fromAdpSqlTypeConverter.convert(columnMetadata.getType(), row.getValue(i)));
        }
        return rowMap;
    }

    private Map<String, Object> createRowMap(Row row, int size) {
        Map<String, Object> rowMap = new HashMap<>();
        for (int i = 0; i < size; i++) {
            rowMap.put(row.getColumnName(i), row.getValue(i));
        }
        return rowMap;
    }

    private Future<Void> execute(PreparedStatementRequest request, SqlConnection connection) {
        return Future.future(promise -> connection.query(request.getSql())
                .execute(rs -> {
                    if (rs.succeeded()) {
                        promise.complete();
                    } else {
                        promise.fail(rs.cause());
                    }
                }));
    }
}
