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
package ru.datamart.prostore.query.execution.plugin.adqm.dml.service;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.cache.service.CacheService;
import ru.datamart.prostore.common.cache.QueryTemplateKey;
import ru.datamart.prostore.common.cache.QueryTemplateValue;
import ru.datamart.prostore.common.dto.QueryParserResponse;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.reader.QueryTemplateResult;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.calcite.core.service.QueryTemplateExtractor;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import ru.datamart.prostore.query.execution.plugin.adqm.base.service.converter.AdqmPluginSpecificLiteralConverter;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.request.LlrRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class AdqmLlrServiceTest {
    private final static String ENRICHED_QUERY = "SELECT * from PSO";
    @Mock
    private QueryEnrichmentService queryEnrichmentService;
    @Mock
    private DatabaseExecutor executorService;
    @Mock
    private CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService;
    @Mock
    private QueryTemplateExtractor templateExtractor;
    @Mock
    private SqlDialect sqlDialect;
    @Mock
    private QueryParserService queryParserService;
    @Mock
    private QueryParserResponse parserResponse;
    @Mock
    private AdqmValidationService adqmValidationService;

    private AdqmLlrService adqmLlrService;

    @BeforeEach
    void setUp() {
        adqmLlrService = new AdqmLlrService(queryEnrichmentService, executorService,
                queryCacheService, templateExtractor, sqlDialect, queryParserService, new AdqmPluginSpecificLiteralConverter(), adqmValidationService);

        lenient().when(queryCacheService.get(any())).thenReturn(null);
        lenient().when(queryParserService.parse(any())).thenReturn(Future.succeededFuture(parserResponse));
        lenient().when(queryEnrichmentService.enrich(any())).thenReturn(Future.succeededFuture(ENRICHED_QUERY));
        lenient().when(templateExtractor.extract(anyString(), any()))
                .thenReturn(new QueryTemplateResult("", null, Collections.emptyList()));
        lenient().when(queryCacheService.put(any(), any()))
                .thenReturn(Future.succeededFuture(QueryTemplateValue.builder().build()));
        HashMap<String, Object> result = new HashMap<>();
        result.put("column", "value");
        lenient().when(executorService.executeWithParams(any(), any(), any()))
                .thenReturn(Future.succeededFuture(Collections.singletonList(result)));
    }

    @Test
    void testExecuteWithoutCacheSuccess(VertxTestContext testContext) {
        // arrange
        List<ColumnMetadata> metadata = Collections.singletonList(ColumnMetadata.builder().build());
        UUID requestId = UUID.randomUUID();
        SqlNode sqlNode = mock(SqlNode.class);
        SqlString sqlString = mock(SqlString.class);
        LlrRequest request = LlrRequest.builder()
                .requestId(requestId)
                .withoutViewsQuery(sqlNode)
                .originalQuery(sqlNode)
                .metadata(metadata)
                .sourceQueryTemplateResult(new QueryTemplateResult("", null, Collections.emptyList()))
                .build();
        when(queryEnrichmentService.getEnrichedSqlNode(any())).thenReturn(Future.succeededFuture(sqlNode));
        when(templateExtractor.enrichTemplate(any(), anyList())).thenReturn(sqlNode);
        when(sqlNode.toSqlString(any(SqlDialect.class))).thenReturn(sqlString);
        when(sqlString.getSql()).thenReturn(ENRICHED_QUERY);
        when(templateExtractor.extract(any(SqlNode.class)))
                .thenReturn(new QueryTemplateResult(ENRICHED_QUERY, sqlNode, Collections.emptyList()));

        // act assert
        adqmLlrService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    if(ar.failed()) {
                        fail(ar.cause());
                    }

                    assertEquals("value", ar.result().getResult().get(0).get("column"));
                    assertEquals(metadata, ar.result().getMetadata());
                    assertEquals(requestId, ar.result().getRequestId());
                    verify(executorService, times(1)).executeWithParams(eq(ENRICHED_QUERY), eq(null), eq(metadata));
                }).completeNow());
    }

    @Test
    void testExecuteEstimate(VertxTestContext testContext) {
        // arrange
        List<ColumnMetadata> metadata = Collections.singletonList(ColumnMetadata.builder().build());
        UUID requestId = UUID.randomUUID();
        SqlNode sqlNode = mock(SqlNode.class);
        SqlString sqlString = mock(SqlString.class);
        LlrRequest request = LlrRequest.builder()
                .requestId(requestId)
                .withoutViewsQuery(sqlNode)
                .originalQuery(sqlNode)
                .metadata(metadata)
                .sourceQueryTemplateResult(new QueryTemplateResult("", null, Collections.emptyList()))
                .estimate(true)
                .build();
        when(queryEnrichmentService.getEnrichedSqlNode(any())).thenReturn(Future.succeededFuture(sqlNode));
        when(sqlString.getSql()).thenReturn(ENRICHED_QUERY);
        when(sqlNode.toSqlString(any(SqlDialect.class))).thenReturn(sqlString);
        when(templateExtractor.extract(any(SqlNode.class)))
                .thenReturn(new QueryTemplateResult(ENRICHED_QUERY, sqlNode, Collections.emptyList()));
        when(templateExtractor.enrichTemplate(any(), anyList())).thenReturn(sqlNode);

        // act assert
        adqmLlrService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    if(ar.failed()) {
                        fail(ar.cause());
                    }

                    QueryResult result = ar.result();
                    assertEquals("{\"plugin\":\"ADQM\",\"estimation\":null,\"query\":\"SELECT * from PSO\"}", result.getResult().get(0).get("estimate"));
                }).completeNow());
    }

    @Test
    void testEnrichQuerySuccess(VertxTestContext testContext) {
        // arrange
        LlrRequest request = LlrRequest.builder().build();
        SqlNode sqlNode = mock(SqlNode.class);
        SqlString sqlString = mock(SqlString.class);
        QueryParserResponse mock = mock(QueryParserResponse.class);
        when(queryEnrichmentService.getEnrichedSqlNode(any())).thenReturn(Future.succeededFuture(sqlNode));
        when(sqlString.getSql()).thenReturn(ENRICHED_QUERY);
        when(sqlNode.toSqlString(any(SqlDialect.class))).thenReturn(sqlString);
        // act assert
        adqmLlrService.enrichQuery(request, mock)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }
                    assertEquals(ENRICHED_QUERY, ar.result().toSqlString(sqlDialect).getSql());
                    verify(queryEnrichmentService, times(1)).getEnrichedSqlNode(any());
                }).completeNow());
    }

    @Test
    void testQueryExecuteSuccess(VertxTestContext testContext) {
        // act assert
        adqmLlrService.queryExecute("", null, Collections.emptyList())
                .onComplete(ar -> testContext.verify(() -> {
                    if(ar.failed()) {
                        fail(ar.cause());
                    }
                    assertEquals("value", ar.result().get(0).get("column"));
                    verify(executorService, times(1)).executeWithParams(any(),
                            eq(null),
                            eq(Collections.emptyList()));
                }).completeNow());
    }
}
