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

import ru.datamart.prostore.common.dml.SelectCategory;
import ru.datamart.prostore.common.dml.ShardingCategory;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.core.dml.dto.PluginDeterminationRequest;
import ru.datamart.prostore.query.execution.core.plugin.configuration.properties.ActivePluginsProperties;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class PluginDeterminationServiceTest {
    private final Set<SourceType> ONE_PLUGIN = new HashSet<>(Collections.singleton(SourceType.ADB));

    @Mock
    private ActivePluginsProperties activePluginsProperties;
    @Mock
    private SelectCategoryQualifier selectCategoryQualifier;
    @Mock
    private ShardingCategoryQualifier shardingCategoryQualifier;
    @Mock
    private SuitablePluginSelector suitablePluginSelector;
    @Mock
    private AcceptableSourceTypesDefinitionService acceptableSourceTypesDefinitionService;
    @InjectMocks
    private PluginDeterminationService pluginDeterminationService;

    @Test
    void shouldGetResultWhenNoCacheDataAndNoPreferredSourceType(VertxTestContext testContext) {
        // arrange
        SqlNode sqlNode = new SqlNodeList(SqlParserPos.ZERO);
        List<Datamart> schema = Collections.emptyList();
        HashSet<SourceType> returnedAcceptablePlugins = new HashSet<>(Arrays.asList(SourceType.ADB, SourceType.ADG));
        when(acceptableSourceTypesDefinitionService.define(same(schema))).thenReturn(Future.succeededFuture(new HashSet<>(returnedAcceptablePlugins)));
        when(suitablePluginSelector.selectByCategory(any(), any(), eq(returnedAcceptablePlugins))).thenReturn(SourceType.ADG);

        // act
        pluginDeterminationService.determine(PluginDeterminationRequest.builder()
                        .sqlNode(sqlNode)
                        .schema(schema)
                        .preferredSourceType(null)
                        .build())
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    assertEquals(returnedAcceptablePlugins, ar.result().getAcceptable());
                    assertEquals(SourceType.ADG, ar.result().getMostSuitable());
                    assertEquals(SourceType.ADG, ar.result().getExecution());
                }).completeNow());
    }

    @Test
    void shouldFailWhenSuitableSelectorFailed(VertxTestContext testContext) {
        // arrange
        SqlNode sqlNode = new SqlNodeList(SqlParserPos.ZERO);
        List<Datamart> schema = Collections.emptyList();
        HashSet<SourceType> returnedAcceptablePlugins = new HashSet<>(Arrays.asList(SourceType.ADB, SourceType.ADG));
        when(acceptableSourceTypesDefinitionService.define(same(schema))).thenReturn(Future.succeededFuture(new HashSet<>(returnedAcceptablePlugins)));
        when(suitablePluginSelector.selectByCategory(any(), any(),  eq(returnedAcceptablePlugins))).thenThrow(new RuntimeException("Exception"));

        // act
        pluginDeterminationService.determine(PluginDeterminationRequest.builder()
                        .sqlNode(sqlNode)
                        .schema(schema)
                        .preferredSourceType(null)
                        .build())
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("Exception", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailWhenAcceptableReturnFailed(VertxTestContext testContext) {
        // arrange
        SqlNode sqlNode = new SqlNodeList(SqlParserPos.ZERO);
        List<Datamart> schema = Collections.emptyList();
        when(acceptableSourceTypesDefinitionService.define(same(schema))).thenReturn(Future.failedFuture(new RuntimeException("Exception")));

        // act
        pluginDeterminationService.determine(PluginDeterminationRequest.builder()
                        .sqlNode(sqlNode)
                        .schema(schema)
                        .preferredSourceType(null)
                        .build())
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("Exception", ar.cause().getMessage());
                    verifyNoInteractions(suitablePluginSelector);
                }).completeNow());
    }

    @Test
    void shouldGetResultWhenNoCacheDataAndPreferredSourceTypeSet(VertxTestContext testContext) {
        // arrange
        SqlNode sqlNode = new SqlNodeList(SqlParserPos.ZERO);
        List<Datamart> schema = Collections.emptyList();
        HashSet<SourceType> returnedAcceptablePlugins = new HashSet<>(Arrays.asList(SourceType.ADB, SourceType.ADG));
        when(acceptableSourceTypesDefinitionService.define(same(schema))).thenReturn(Future.succeededFuture(new HashSet<>(returnedAcceptablePlugins)));
        when(suitablePluginSelector.selectByCategory(any()
                , any(), eq(returnedAcceptablePlugins))).thenReturn(SourceType.ADG);

        // act
        pluginDeterminationService.determine(PluginDeterminationRequest.builder()
                        .sqlNode(sqlNode)
                        .schema(schema)
                        .preferredSourceType(SourceType.ADB)
                        .build())
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    assertEquals(returnedAcceptablePlugins, ar.result().getAcceptable());
                    assertEquals(SourceType.ADG, ar.result().getMostSuitable());
                    assertEquals(SourceType.ADB, ar.result().getExecution());
                }).completeNow());
    }

    @Test
    void shouldGetResultWhenMostSuitableCachedAndNoPreferredSourceType(VertxTestContext testContext) {
        // arrange
        SqlNode sqlNode = new SqlNodeList(SqlParserPos.ZERO);
        List<Datamart> schema = Collections.emptyList();
        HashSet<SourceType> returnedAcceptablePlugins = new HashSet<>(Arrays.asList(SourceType.ADB, SourceType.ADG));
        when(acceptableSourceTypesDefinitionService.define(same(schema))).thenReturn(Future.succeededFuture(new HashSet<>(returnedAcceptablePlugins)));

        // act
        pluginDeterminationService.determine(PluginDeterminationRequest.builder()
                        .sqlNode(sqlNode)
                        .schema(schema)
                        .preferredSourceType(null)
                        .cachedMostSuitablePlugin(SourceType.ADB)
                        .build())
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    assertEquals(returnedAcceptablePlugins, ar.result().getAcceptable());
                    assertEquals(SourceType.ADB, ar.result().getMostSuitable());
                    assertEquals(SourceType.ADB, ar.result().getExecution());
                    verifyNoInteractions(suitablePluginSelector);
                }).completeNow());
    }

    @Test
    void shouldGetResultWhenMostSuitableCachedAndNoPreferredSourceTypeSet(VertxTestContext testContext) {
        // arrange
        SqlNode sqlNode = new SqlNodeList(SqlParserPos.ZERO);
        List<Datamart> schema = Collections.emptyList();
        HashSet<SourceType> returnedAcceptablePlugins = new HashSet<>(Arrays.asList(SourceType.ADB, SourceType.ADG));
        when(acceptableSourceTypesDefinitionService.define(same(schema))).thenReturn(Future.succeededFuture(new HashSet<>(returnedAcceptablePlugins)));

        // act
        pluginDeterminationService.determine(PluginDeterminationRequest.builder()
                        .sqlNode(sqlNode)
                        .schema(schema)
                        .preferredSourceType(SourceType.ADG)
                        .cachedMostSuitablePlugin(SourceType.ADB)
                        .build())
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    assertEquals(returnedAcceptablePlugins, ar.result().getAcceptable());
                    assertEquals(SourceType.ADB, ar.result().getMostSuitable());
                    assertEquals(SourceType.ADG, ar.result().getExecution());
                    verifyNoInteractions(suitablePluginSelector);
                }).completeNow());
    }

    @Test
    void shouldFailWhenMostSuitableCachedAndNoPreferredSourceTypeSetAndNotAcceptable(VertxTestContext testContext) {
        // arrange
        SqlNode sqlNode = new SqlNodeList(SqlParserPos.ZERO);
        List<Datamart> schema = Collections.emptyList();
        HashSet<SourceType> returnedAcceptablePlugins = new HashSet<>(Arrays.asList(SourceType.ADB, SourceType.ADG));
        when(acceptableSourceTypesDefinitionService.define(same(schema))).thenReturn(Future.succeededFuture(new HashSet<>(returnedAcceptablePlugins)));

        // act
        pluginDeterminationService.determine(PluginDeterminationRequest.builder()
                        .sqlNode(sqlNode)
                        .schema(schema)
                        .preferredSourceType(SourceType.ADQM)
                        .cachedMostSuitablePlugin(SourceType.ADB)
                        .build())
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("Queried entity is missing for the specified DATASOURCE_TYPE ADQM", ar.cause().getMessage());
                    verifyNoInteractions(suitablePluginSelector);
                }).completeNow());
    }

    @Test
    void shouldGetResultWhenEmptyCachedAcceptablePluginsAndNoPreferredSourceType(VertxTestContext testContext) {
        // arrange
        SqlNode sqlNode = new SqlNodeList(SqlParserPos.ZERO);
        List<Datamart> schema = Collections.emptyList();
        HashSet<SourceType> returnedAcceptablePlugins = new HashSet<>(Arrays.asList(SourceType.ADB, SourceType.ADG));
        when(acceptableSourceTypesDefinitionService.define(same(schema))).thenReturn(Future.succeededFuture(new HashSet<>(returnedAcceptablePlugins)));
        when(suitablePluginSelector.selectByCategory(any(), any(), eq(returnedAcceptablePlugins))).thenReturn(SourceType.ADG);

        // act
        pluginDeterminationService.determine(PluginDeterminationRequest.builder()
                        .sqlNode(sqlNode)
                        .schema(schema)
                        .preferredSourceType(null)
                        .cachedAcceptablePlugins(Collections.emptySet())
                        .build())
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    assertEquals(returnedAcceptablePlugins, ar.result().getAcceptable());
                    assertEquals(SourceType.ADG, ar.result().getMostSuitable());
                    assertEquals(SourceType.ADG, ar.result().getExecution());
                }).completeNow());
    }

    @Test
    void shouldGetResultWhenCachedAcceptablePluginsAndNoPreferredSourceType(VertxTestContext testContext) {
        // arrange
        SqlNode sqlNode = new SqlNodeList(SqlParserPos.ZERO);
        List<Datamart> schema = Collections.emptyList();
        HashSet<SourceType> cachedAcceptablePlugins = new HashSet<>(Arrays.asList(SourceType.ADB, SourceType.ADG));
        when(suitablePluginSelector.selectByCategory(any(), any(), eq(cachedAcceptablePlugins))).thenReturn(SourceType.ADG);

        // act
        pluginDeterminationService.determine(PluginDeterminationRequest.builder()
                        .sqlNode(sqlNode)
                        .schema(schema)
                        .preferredSourceType(null)
                        .cachedAcceptablePlugins(cachedAcceptablePlugins)
                        .build())
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    assertEquals(cachedAcceptablePlugins, ar.result().getAcceptable());
                    assertEquals(SourceType.ADG, ar.result().getMostSuitable());
                    assertEquals(SourceType.ADG, ar.result().getExecution());

                    verifyNoInteractions(acceptableSourceTypesDefinitionService);
                }).completeNow());
    }

    @Test
    void shouldGetResultWhenOnlyOneActivePluginAndNoPreferredSourceType(VertxTestContext testContext) {
        // arrange
        SqlNode sqlNode = new SqlNodeList(SqlParserPos.ZERO);
        List<Datamart> schema = Collections.emptyList();
        when(activePluginsProperties.getActive()).thenReturn(ONE_PLUGIN);

        // act
        pluginDeterminationService.determine(PluginDeterminationRequest.builder()
                .sqlNode(sqlNode)
                .schema(schema)
                .preferredSourceType(null)
                .build())
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    assertEquals(ONE_PLUGIN, ar.result().getAcceptable());
                    assertEquals(SourceType.ADB, ar.result().getMostSuitable());
                    assertEquals(SourceType.ADB, ar.result().getExecution());

                    verifyNoInteractions(suitablePluginSelector);
                    verifyNoInteractions(acceptableSourceTypesDefinitionService);
                }).completeNow());
    }

    @Test
    void shouldGetResultWhenOnlyOneActivePluginAndSamePreferredSourceType(VertxTestContext testContext) {
        // arrange
        SqlNode sqlNode = new SqlNodeList(SqlParserPos.ZERO);
        List<Datamart> schema = Collections.emptyList();
        when(activePluginsProperties.getActive()).thenReturn(ONE_PLUGIN);

        // act
        pluginDeterminationService.determine(PluginDeterminationRequest.builder()
                .sqlNode(sqlNode)
                .schema(schema)
                .preferredSourceType(SourceType.ADB)
                .build())
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    assertEquals(ONE_PLUGIN, ar.result().getAcceptable());
                    assertEquals(SourceType.ADB, ar.result().getMostSuitable());
                    assertEquals(SourceType.ADB, ar.result().getExecution());

                    verifyNoInteractions(suitablePluginSelector);
                    verifyNoInteractions(acceptableSourceTypesDefinitionService);
                }).completeNow());
    }

    @Test
    void shouldFailWhenOnlyOneActivePluginAndDifferentPreferredSourceType(VertxTestContext testContext) {
        // arrange
        SqlNode sqlNode = new SqlNodeList(SqlParserPos.ZERO);
        List<Datamart> schema = Collections.emptyList();
        when(activePluginsProperties.getActive()).thenReturn(ONE_PLUGIN);

        // act
        pluginDeterminationService.determine(PluginDeterminationRequest.builder()
                .sqlNode(sqlNode)
                .schema(schema)
                .preferredSourceType(SourceType.ADG)
                .build())
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("Queried entity is missing for the specified DATASOURCE_TYPE ADG", ar.cause().getMessage());
                    verifyNoInteractions(suitablePluginSelector);
                    verifyNoInteractions(acceptableSourceTypesDefinitionService);
                }).completeNow());
    }
}