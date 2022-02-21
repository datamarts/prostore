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
package ru.datamart.prostore.query.execution.plugin.adg.query.service;

import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.async.AsyncUtils;
import ru.datamart.prostore.common.converter.SqlTypeConverter;
import ru.datamart.prostore.common.reader.QueryParameters;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import ru.datamart.prostore.query.execution.plugin.adg.db.verticle.AdgQueryExecutorVerticle;
import ru.datamart.prostore.query.execution.plugin.api.exception.DataSourceException;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@Service("adgQueryExecutor")
public class AdgQueryExecutorService implements QueryExecutorService {
    private final AdgQueryExecutorVerticle adgQueryExecutorVerticle;
    private final SqlTypeConverter fromAdgSqlTypeConverter;
    private final SqlTypeConverter toAdgSqlTypeConverter;

    @Autowired
    public AdgQueryExecutorService(AdgQueryExecutorVerticle adgQueryExecutorVerticle,
                                   @Qualifier("fromAdgSqlTypeConverter") SqlTypeConverter fromAdgSqlTypeConverter,
                                   @Qualifier("toAdgSqlTypeConverter") SqlTypeConverter toAdgSqlTypeConverter) {
        this.adgQueryExecutorVerticle = adgQueryExecutorVerticle;
        this.fromAdgSqlTypeConverter = fromAdgSqlTypeConverter;
        this.toAdgSqlTypeConverter = toAdgSqlTypeConverter;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Future<List<Map<String, Object>>> execute(String sql,
                                                     QueryParameters queryParameters,
                                                     List<ColumnMetadata> queryMetadata) {
        return Future.future(promise -> {
            List<Object> paramsList = createParamsList(queryParameters);
            log.debug("ADG. Execute query [{}]", sql);
            AsyncUtils.measureMs(adgQueryExecutorVerticle.callQuery(sql, paramsList.toArray()),
                    duration -> log.debug("ADG. Query completed successfully: [{}] in [{}]ms", sql, duration))
                    .onComplete(ar -> {
                        if (ar.succeeded() && ar.result() != null && !ar.result().isEmpty()) {
                            val map = (Map<?, ?>) ar.result().get(0);
                            val dataSet = (List<List<?>>) map.get("rows");
                            final List<Map<String, Object>> result = new ArrayList<>();
                            try {
                                dataSet.forEach(row -> {
                                    val rowMap = createRowMap(queryMetadata, row);
                                    result.add(rowMap);
                                });
                            } catch (Exception e) {
                                promise.fail(
                                        new DataSourceException("Error converting value to jdbc type", e));
                                return;
                            }
                            promise.complete(result);
                        } else {
                            promise.fail(ar.cause());
                        }
                    });
        });
    }

    @Override
    public Future<Void> executeUpdate(String sql, QueryParameters queryParameters) {
        List<Object> paramsList = createParamsList(queryParameters);
        log.debug("ADG. Execute query [{}]", sql);
        return AsyncUtils.measureMs(adgQueryExecutorVerticle.callQuery(sql, paramsList.toArray()),
                duration -> log.debug("ADG. Query completed successfully: [{}] in [{}]ms", sql, duration))
                .mapEmpty();
    }

    private List<Object> createParamsList(QueryParameters params) {
        if (params == null) {
            return Collections.emptyList();
        } else {
            return IntStream.range(0, params.getValues().size())
                    .mapToObj(n -> toAdgSqlTypeConverter.convert(params.getTypes().get(n),
                            params.getValues().get(n)))
                    .collect(Collectors.toList());
        }
    }

    private Map<String, Object> createRowMap(List<ColumnMetadata> metadata, List<?> row) {
        Map<String, Object> rowMap = new HashMap<>();
        for (int i = 0; i < row.size(); i++) {
            final ColumnMetadata columnMetadata = metadata.get(i);
            rowMap.put(columnMetadata.getName(), fromAdgSqlTypeConverter.convert(columnMetadata.getType(), row.get(i)));
        }
        return rowMap;
    }
}
