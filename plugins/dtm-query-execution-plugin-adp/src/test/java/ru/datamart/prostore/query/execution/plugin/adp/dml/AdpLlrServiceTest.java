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
package ru.datamart.prostore.query.execution.plugin.adp.dml;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.cache.service.CacheService;
import ru.datamart.prostore.common.cache.QueryTemplateKey;
import ru.datamart.prostore.common.cache.QueryTemplateValue;
import ru.datamart.prostore.common.delta.DeltaInformation;
import ru.datamart.prostore.common.delta.DeltaType;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.QueryParameters;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.reader.QueryTemplateResult;
import ru.datamart.prostore.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.adp.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.plugin.adp.calcite.factory.AdpCalciteSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adp.calcite.factory.AdpSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adp.calcite.service.AdpCalciteContextProvider;
import ru.datamart.prostore.query.execution.plugin.adp.calcite.service.AdpCalciteDMLQueryParserService;
import ru.datamart.prostore.query.execution.plugin.adp.calcite.service.AdpCalciteDefinitionService;
import ru.datamart.prostore.query.execution.plugin.adp.db.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adp.enrichment.service.AdpDmlQueryExtendService;
import ru.datamart.prostore.query.execution.plugin.adp.enrichment.service.AdpQueryEnrichmentService;
import ru.datamart.prostore.query.execution.plugin.adp.enrichment.service.AdpSchemaExtender;
import ru.datamart.prostore.query.execution.plugin.api.dml.LlrEstimateUtils;
import ru.datamart.prostore.query.execution.plugin.api.request.LlrRequest;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
class AdpLlrServiceTest {
    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final SqlParserImplFactory factory = calciteConfiguration.ddlParserImplFactory();
    private final SqlParser.Config configParser = calciteConfiguration.configDdlParser(factory);
    private final AdpCalciteDefinitionService definitionService = new AdpCalciteDefinitionService(configParser);
    private final SqlDialect sqlDialect = calciteConfiguration.adpSqlDialect();
    private final DtmRelToSqlConverter relToSqlConverter = new DtmRelToSqlConverter(sqlDialect);

    @Mock
    private DatabaseExecutor databaseExecutor;

    @Mock
    private CacheService<QueryTemplateKey, QueryTemplateValue> cacheService;

    @Captor
    private ArgumentCaptor<String> sqlArgumentCaptor;

    private AdpLlrService adpLlrService;

    @BeforeEach
    void setUp(Vertx vertx) {
        AdpCalciteSchemaFactory calciteSchemaFactory = new AdpCalciteSchemaFactory(new AdpSchemaFactory());
        AdpDmlQueryExtendService queryExtendService = new AdpDmlQueryExtendService();
        AdpCalciteContextProvider contextProvider = new AdpCalciteContextProvider(configParser, calciteSchemaFactory);
        AdpSchemaExtender schemaExtender = new AdpSchemaExtender();
        AdpQueryEnrichmentService adpQueryEnrichmentService = new AdpQueryEnrichmentService(queryExtendService, sqlDialect, relToSqlConverter);
        AdpCalciteDMLQueryParserService queryParserService = new AdpCalciteDMLQueryParserService(contextProvider, vertx, schemaExtender);
        AdpQueryTemplateExtractor templateExtractor = new AdpQueryTemplateExtractor(definitionService, sqlDialect);
        adpLlrService = new AdpLlrService(
                adpQueryEnrichmentService,
                databaseExecutor,
                cacheService,
                templateExtractor,
                sqlDialect,
                queryParserService
        );

        when(cacheService.put(any(), any())).thenAnswer(invocation -> Future.succeededFuture(invocation.getArgument(1)));
        when(databaseExecutor.executeWithParams(any(), any(), any())).thenReturn(Future.succeededFuture(Collections.emptyList()));
    }

    @Test
    void shouldSuccessWhenValidQuery(VertxTestContext testContext) {
        // arrange
        String query = "SELECT * FROM datamart.tbl";

        List<Datamart> schema = Collections.singletonList(
                new Datamart("datamart", false, Arrays.asList(
                        Entity.builder()
                                .name("tbl")
                                .entityType(EntityType.TABLE)
                                .fields(Arrays.asList(
                                        EntityField.builder()
                                                .name("id")
                                                .type(ColumnType.BIGINT)
                                                .ordinalPosition(0)
                                                .primaryOrder(1)
                                                .build()
                                ))
                                .build()
                )));
        UUID uuid = UUID.randomUUID();
        SqlNode sqlNode = definitionService.processingQuery(query);
        QueryTemplateResult queryTemplateResult = new QueryTemplateResult(query, sqlNode, Collections.emptyList());
        List<ColumnMetadata> metadata = Collections.emptyList();
        QueryParameters parameters = new QueryParameters();
        LlrRequest llrRequest = LlrRequest.builder()
                .sourceQueryTemplateResult(queryTemplateResult)
                .withoutViewsQuery(sqlNode)
                .originalQuery(sqlNode)
                .requestId(uuid)
                .envName("test")
                .metadata(metadata)
                .schema(schema)
                .deltaInformations(Arrays.asList(
                        DeltaInformation.builder()
                                .type(DeltaType.NUM)
                                .selectOnNum(0L)
                                .build()
                ))
                .parameters(parameters)
                .datamartMnemonic("datamart")
                .build();

        // act
        Future<QueryResult> execute = adpLlrService.execute(llrRequest);

        // assert
        execute.onComplete(ar -> testContext.verify(() -> {
            if (ar.failed()) {
                fail(ar.cause());
            }
            assertTrue(ar.succeeded());

            verify(cacheService).get(Mockito.any());
            verify(cacheService).put(Mockito.any(), Mockito.any());
            verify(databaseExecutor).executeWithParams(sqlArgumentCaptor.capture(), same(parameters), same(metadata));
            String sql = sqlArgumentCaptor.getValue();
            assertThat(sql).isEqualToNormalizingNewlines("SELECT id\n" +
                    "FROM datamart.tbl_actual\n" +
                    "WHERE sys_from <= 0 AND COALESCE(sys_to, 9223372036854775807) >= 0");
        }).completeNow());
    }

    @Test
    void executeEstimateQuery(VertxTestContext testContext) {
        // arrange
        String query = "SELECT * FROM datamart.tbl";

        List<Datamart> schema = Collections.singletonList(
                new Datamart("datamart", false, Arrays.asList(
                        Entity.builder()
                                .name("tbl")
                                .entityType(EntityType.TABLE)
                                .fields(Arrays.asList(
                                        EntityField.builder()
                                                .name("id")
                                                .type(ColumnType.BIGINT)
                                                .ordinalPosition(0)
                                                .primaryOrder(1)
                                                .build()
                                ))
                                .build()
                )));
        UUID uuid = UUID.randomUUID();
        SqlNode sqlNode = definitionService.processingQuery(query);
        QueryTemplateResult queryTemplateResult = new QueryTemplateResult(query, sqlNode, Collections.emptyList());
        List<ColumnMetadata> metadata = Collections.emptyList();
        QueryParameters parameters = new QueryParameters();
        LlrRequest llrRequest = LlrRequest.builder()
                .sourceQueryTemplateResult(queryTemplateResult)
                .withoutViewsQuery(sqlNode)
                .originalQuery(sqlNode)
                .requestId(uuid)
                .envName("test")
                .metadata(metadata)
                .schema(schema)
                .deltaInformations(Arrays.asList(
                        DeltaInformation.builder()
                                .type(DeltaType.NUM)
                                .selectOnNum(0L)
                                .build()
                ))
                .parameters(parameters)
                .datamartMnemonic("datamart")
                .estimate(true)
                .build();

        String json = "[{\"test\":true}]";
        HashMap<String, Object> item = new HashMap<>();
        JsonArray jsonArray = new JsonArray(json);
        item.put(LlrEstimateUtils.LLR_ESTIMATE_METADATA.getName(), jsonArray);

        when(databaseExecutor.executeWithParams(any(), any(), any()))
                .thenReturn(Future.succeededFuture(Arrays.asList(item)));

        // act
        Future<QueryResult> execute = adpLlrService.execute(llrRequest);

        // assert
        execute.onComplete(ar -> testContext.verify(() -> {
            if (ar.failed()) {
                fail(ar.cause());
            }
            assertTrue(ar.succeeded());

            verify(cacheService).get(Mockito.any());
            verify(cacheService).put(Mockito.any(), Mockito.any());
            verify(databaseExecutor).executeWithParams(sqlArgumentCaptor.capture(), same(parameters), any());
            String sql = sqlArgumentCaptor.getValue();
            assertThat(sql).isEqualToNormalizingNewlines("EXPLAIN (FORMAT JSON) SELECT id\n" +
                    "FROM datamart.tbl_actual\n" +
                    "WHERE sys_from <= 0 AND COALESCE(sys_to, 9223372036854775807) >= 0");

            QueryResult result = ar.result();
            assertEquals("{\"plugin\":\"ADP\",\"estimation\":[{\"test\":true}],\"query\":\"SELECT id FROM datamart.tbl_actual WHERE sys_from <= 0 AND COALESCE(sys_to, 9223372036854775807) >= 0\"}",
                    result.getResult().get(0).get("estimate"));
        }).completeNow());
    }
}