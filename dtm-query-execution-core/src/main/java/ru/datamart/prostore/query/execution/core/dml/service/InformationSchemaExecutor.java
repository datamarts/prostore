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
package ru.datamart.prostore.query.execution.core.dml.service;

import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.reader.InformationSchemaView;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.reader.QuerySourceRequest;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.execution.core.base.service.hsql.HSQLClient;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class InformationSchemaExecutor {
    private final SqlDialect coreSqlDialect;
    private final QueryParserService parserService;
    private final HSQLClient client;

    @Autowired
    public InformationSchemaExecutor(HSQLClient client,
                                     @Qualifier("coreSqlDialect") SqlDialect coreSqlDialect,
                                     @Qualifier("coreCalciteDMLQueryParserService") QueryParserService parserService) {
        this.client = client;
        this.coreSqlDialect = coreSqlDialect;
        this.parserService = parserService;
    }

    public Future<QueryResult> execute(QuerySourceRequest request) {
        return executeInternal(request);
    }

    private Future<QueryResult> executeInternal(QuerySourceRequest request) {
        return Future.future(promise -> {
            getEnrichmentQuerySql(request)
                    .onSuccess(query -> client.getQueryResult(query)
                            .onSuccess(resultSet -> {
                                val result = resultSet.getRows().stream()
                                        .map(row -> toResultRow(resultSet.getColumnNames(), row, request.getMetadata()))
                                        .collect(Collectors.toList());
                                promise.complete(
                                        QueryResult.builder()
                                                .requestId(request.getQueryRequest().getRequestId())
                                                .metadata(request.getMetadata())
                                                .result(result)
                                                .build());
                            })
                            .onFailure(promise::fail))
                    .onFailure(promise::fail);
        });
    }

    private Map<String, Object> toResultRow(List<String> columnNames, JsonObject row, List<ColumnMetadata> metadata) {
        Map<String, Object> map = row.getMap();
        Map<String, Object> resultRow = new HashMap<>();
        for (int i = 0; i < metadata.size(); i++) {
            resultRow.put(metadata.get(i).getName(), map.get(columnNames.get(i)));
        }
        return resultRow;
    }

    private Future<String> getEnrichmentQuerySql(QuerySourceRequest request) {
        return Future.future(p -> {
                    toUpperCase(request);
                    val parserRequest = new QueryParserRequest(request.getQuery(), request.getLogicalSchema());
                    parserService.parse(parserRequest)
                            .map(response -> {
                                val enrichmentNode = response.getSqlNode();
                                return enrichmentNode.toSqlString(coreSqlDialect).getSql()
                                        .replace(InformationSchemaView.SCHEMA_NAME.toLowerCase(),
                                                InformationSchemaView.DTM_SCHEMA_NAME.toLowerCase());
                            })
                            .onComplete(p);
                }
        );
    }

    private void toUpperCase(QuerySourceRequest request) {
        request.getMetadata()
                .forEach(c -> c.setName(c.getName().toUpperCase()));
    }
}
