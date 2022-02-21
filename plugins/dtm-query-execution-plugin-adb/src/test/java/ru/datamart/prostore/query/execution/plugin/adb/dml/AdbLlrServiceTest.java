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
package ru.datamart.prostore.query.execution.plugin.adb.dml;

import ru.datamart.prostore.cache.service.CacheService;
import ru.datamart.prostore.common.cache.QueryTemplateKey;
import ru.datamart.prostore.common.cache.QueryTemplateValue;
import ru.datamart.prostore.common.dto.QueryParserResponse;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.reader.QueryTemplateResult;
import ru.datamart.prostore.query.calcite.core.dialect.LimitSqlDialect;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.calcite.core.service.QueryTemplateExtractor;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.adb.dml.service.AdbLlrService;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adb.utils.TestUtils;
import ru.datamart.prostore.query.execution.plugin.api.dml.LlrEstimateUtils;
import ru.datamart.prostore.query.execution.plugin.api.request.LlrRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.LlrService;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@Slf4j
@ExtendWith({MockitoExtension.class, VertxExtension.class})
class AdbLlrServiceTest {
    private final static String template = "SELECT * from PSO";
    private LlrService<QueryResult> adbLLRService;

    @Mock
    private QueryEnrichmentService adbQueryEnrichmentService;
    @Mock
    private DatabaseExecutor adbDatabaseExecutor;
    @Mock
    private QueryTemplateResult queryTemplateResult;
    @Mock
    private QueryTemplateExtractor queryTemplateExtractor;
    @Mock
    private CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService;
    @Mock
    private QueryParserService queryParserService;
    @Mock
    private QueryParserResponse parserResponse;
    @Captor
    private ArgumentCaptor<String> queryCaptor;


    @BeforeEach
    void init() {
        when(queryTemplateResult.getTemplate()).thenReturn(template);
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(template);
        when(adbQueryEnrichmentService.getEnrichedSqlNode(any(), any()))
                .thenReturn(Future.succeededFuture(sqlNode));
        when(queryTemplateExtractor.extract(any(SqlNode.class))).thenReturn(queryTemplateResult);
        when(queryTemplateExtractor.enrichTemplate(any(), anyList())).thenReturn(sqlNode);
        when(queryCacheService.put(any(), any())).thenReturn(Future.succeededFuture());
        when(queryParserService.parse(any())).thenReturn(Future.succeededFuture(parserResponse));
        adbLLRService = new AdbLlrService(adbQueryEnrichmentService,
                adbDatabaseExecutor,
                queryCacheService,
                queryTemplateExtractor,
                new LimitSqlDialect(SqlDialect.EMPTY_CONTEXT
                        .withDatabaseProduct(SqlDialect.DatabaseProduct.POSTGRESQL)
                        .withIdentifierQuoteString("")
                        .withUnquotedCasing(Casing.TO_LOWER)
                        .withCaseSensitive(false)
                        .withQuotedCasing(Casing.UNCHANGED)),
                queryParserService);
    }

    @Test
    void executeQuery(VertxTestContext testContext) {
        // arrange
        when(adbDatabaseExecutor.executeWithParams(any(), any(), any()))
                .thenReturn(Future.succeededFuture(new ArrayList<>()));

        List<Datamart> schema = Collections.singletonList(
                new Datamart("TEST_DATAMART", false, Collections.emptyList()));
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql(template);
        UUID uuid = UUID.randomUUID();
        queryRequest.setRequestId(uuid);
        queryRequest.setDatamartMnemonic("TEST_DATAMART");
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(template);
        QueryTemplateResult queryTemplateResult = new QueryTemplateResult(template, sqlNode, Collections.emptyList());
        LlrRequest llrRequest = LlrRequest.builder()
                .sourceQueryTemplateResult(queryTemplateResult)
                .withoutViewsQuery(sqlNode)
                .originalQuery(sqlNode)
                .requestId(uuid)
                .envName("test")
                .metadata(Collections.emptyList())
                .schema(schema)
                .deltaInformations(Collections.emptyList())
                .datamartMnemonic("TEST_DATAMART")
                .build();

        // act assert
        adbLLRService.execute(llrRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    QueryResult result = ar.result();
                    assertEquals(uuid, result.getRequestId());
                }).completeNow());
    }

    @Test
    void executeEstimateQuery() {
        // arrange
        String json = "[{\"test\":true}]";
        HashMap<String, Object> item = new HashMap<>();
        JsonArray jsonArray = new JsonArray(json);
        item.put(LlrEstimateUtils.LLR_ESTIMATE_METADATA.getName(), jsonArray);

        when(adbDatabaseExecutor.executeWithParams(any(), any(), any()))
                .thenReturn(Future.succeededFuture(Arrays.asList(item)));

        List<Datamart> schema = Collections.singletonList(
                new Datamart("TEST_DATAMART", false, Collections.emptyList()));
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql(template);
        UUID uuid = UUID.randomUUID();
        queryRequest.setRequestId(uuid);
        queryRequest.setDatamartMnemonic("TEST_DATAMART");
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(template);
        QueryTemplateResult queryTemplateResult = new QueryTemplateResult(template, sqlNode, Collections.emptyList());
        LlrRequest llrRequest = LlrRequest.builder()
                .sourceQueryTemplateResult(queryTemplateResult)
                .withoutViewsQuery(sqlNode)
                .originalQuery(sqlNode)
                .requestId(uuid)
                .envName("test")
                .metadata(Collections.emptyList())
                .schema(schema)
                .deltaInformations(Collections.emptyList())
                .datamartMnemonic("TEST_DATAMART")
                .estimate(true)
                .build();

        // act assert
        adbLLRService.execute(llrRequest)
                .onComplete(ar -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    verify(adbDatabaseExecutor).executeWithParams(queryCaptor.capture(), any(), any());
                    String query = queryCaptor.getValue();
                    Assertions.assertThat(query).isEqualToNormalizingNewlines("EXPLAIN (FORMAT JSON) SELECT *\nFROM pso");

                    QueryResult result = ar.result();
                    assertEquals(uuid, result.getRequestId());
                    assertEquals("{\"plugin\":\"ADB\",\"estimation\":[{\"test\":true}],\"query\":\"SELECT * FROM pso\"}", result.getResult().get(0).get("estimate"));
                });
    }
}
