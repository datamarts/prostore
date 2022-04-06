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
package ru.datamart.prostore.query.execution.plugin.adg.mppw.kafka.service;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.internal.verification.VerificationModeFactory;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.common.dto.KafkaBrokerInfo;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.ExternalTableFormat;
import ru.datamart.prostore.query.execution.plugin.adg.base.factory.AdgHelperTableNamesFactory;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.response.AdgCartridgeError;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import ru.datamart.prostore.query.execution.plugin.adg.mppw.configuration.properties.AdgMppwProperties;
import ru.datamart.prostore.query.execution.plugin.api.exception.DataSourceException;
import ru.datamart.prostore.query.execution.plugin.api.mppw.MppwRequest;
import ru.datamart.prostore.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import ru.datamart.prostore.query.execution.plugin.api.mppw.kafka.UploadExternalEntityMetadata;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class AdgMppwKafkaServiceTest {

    private static final String CONSUMER_GROUP = "consumerGroup";
    public static final String STANDALONE_SPACE_NAME = "spaceName";
    public static final int UPLOAD_MESSAGE_LIMIT = 1000;
    public static final String TOPIC_NAME = "topic1";
    @Mock
    private AdgCartridgeClient client;
    private AdgMppwKafkaService service;

    @BeforeEach
    public void before() {
        val tableNamesFactory = new AdgHelperTableNamesFactory();
        val mppwKafkaProperties = new AdgMppwProperties();
        mppwKafkaProperties.setMaxNumberOfMessagesPerPartition(200);
        mppwKafkaProperties.setConsumerGroup(CONSUMER_GROUP);
        service = new AdgMppwKafkaService(
                client,
                mppwKafkaProperties,
                tableNamesFactory);
    }

    @Test
    void allGoodInitTest(VertxTestContext testContext) {
        // arrange
        val context = getRequestContext(1L);
        when(client.subscribe(any())).thenReturn(Future.succeededFuture());

        // act
        service.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }
                    verify(client).subscribe(argThat(argument -> {
                        assertThat(argument.getSpaceNames(), Matchers.contains("env1__test__tbl1_staging"));
                        assertEquals(UPLOAD_MESSAGE_LIMIT, argument.getMaxNumberOfMessagesPerPartition());
                        assertEquals(argument.getTopicName(), TOPIC_NAME);
                        return true;
                    }));
                    verifyNoMoreInteractions(client);
                }).completeNow());
    }

    @Test
    void testMaxNumberOfMessagesFromEntity(VertxTestContext testContext) {
        // arrange
        val context = getRequestContext(1L);
        long maxNumberOfMessages = 300L;
        context.getSourceEntity().setExternalTableUploadMessageLimit((int) maxNumberOfMessages);
        when(client.subscribe(any())).thenReturn(Future.succeededFuture());

        // act
        service.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }
                    verify(client, VerificationModeFactory.times(1)).subscribe(
                            argThat(request -> maxNumberOfMessages == request.getMaxNumberOfMessagesPerPartition()));
                    verifyNoMoreInteractions(client);
                }).completeNow());
    }

    @Test
    void testMaxNumberOfMessagesFromProperties(VertxTestContext testContext) {
        // arrange
        val context = getRequestContext(1L);
        context.getSourceEntity().setExternalTableUploadMessageLimit(null);
        when(client.subscribe(any())).thenReturn(Future.succeededFuture());

        // act
        service.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }
                    verify(client).subscribe(argThat(request -> 200L == request.getMaxNumberOfMessagesPerPartition()));
                    verifyNoMoreInteractions(client);
                }).completeNow());
    }

    @Test
    void allGoodCancelTest(VertxTestContext testContext) {
        // arrange
        val context = getRequestContext(1L);
        context.setLoadStart(false);
        when(client.transferDataToScdTable(any())).thenReturn(Future.succeededFuture());
        when(client.cancelSubscription(any())).thenReturn(Future.succeededFuture());

        // act
        service.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }
                    assertNull(ar.result());
                    verify(client).cancelSubscription(any());
                    verifyNoMoreInteractions(client);
                }).completeNow());
    }

    @Test
    void badSubscriptionTest(VertxTestContext testContext) {
        // arrange
        val context = getRequestContext(1L);
        when(client.subscribe(any()))
                .thenReturn(Future.failedFuture(new DataSourceException("subscribe error")));

        // act
        service.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                    verify(client, VerificationModeFactory.times(1)).subscribe(any());
                    verifyNoMoreInteractions(client);
                }).completeNow());
    }

    @Test
    void badSubscriptionTest2(VertxTestContext testContext) {
        // arrange
        val context = getRequestContext(1L);
        when(client.subscribe(any()))
                .thenReturn(Future.failedFuture(new AdgCartridgeError("error", "connector error")));

        // act
        service.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    /// assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                    verify(client).subscribe(any());
                    verifyNoMoreInteractions(client);
                }).completeNow());
    }

    @Test
    void badTransferDataTest(VertxTestContext testContext) {
        // arrange
        val context = getRequestContext(1L);
        context.setLoadStart(false);
        when(client.transferDataToScdTable(any()))
                .thenReturn(Future.failedFuture(new DataSourceException("transferDataToScdTable error")));

        // act
        service.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                    verify(client).transferDataToScdTable(any());
                    verifyNoMoreInteractions(client);
                }).completeNow());
    }

    @Test
    void badCancelTest(VertxTestContext testContext) {
        // arrange
        val context = getRequestContext(1L);
        context.setLoadStart(false);
        when(client.transferDataToScdTable(any()))
                .thenReturn(Future.succeededFuture());
        when(client.cancelSubscription(any()))
                .thenReturn(Future.failedFuture(new AdgCartridgeError("error", "connector error")));

        // act
        service.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                    verify(client).transferDataToScdTable(any());
                    verifyNoMoreInteractions(client);
                }).completeNow());
    }

    @Test
    void goodAndBadTransferDataTest(VertxTestContext testContext) {
        // arrange
        val context = getRequestContext(1L);
        when(client.subscribe(any())).thenReturn(Future.succeededFuture());
        when(client.transferDataToScdTable(any())).thenReturn(Future.succeededFuture());
        service.execute(context)
                .onComplete(ar -> assertTrue(ar.succeeded()));
        when(client.transferDataToScdTable(any()))
                .thenReturn(Future.failedFuture(new DataSourceException("transferDataToScdTable error")));

        // act
        service.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                    verify(client).subscribe(any());
                    verify(client).transferDataToScdTable(any());
                    verifyNoMoreInteractions(client);
                }).completeNow());
    }

    @Test
    void good2TransferDataTest(VertxTestContext testContext) {
        // arrange
        val context = getRequestContext(1L);
        when(client.subscribe(any())).thenReturn(Future.succeededFuture());
        when(client.transferDataToScdTable(any())).thenReturn(Future.succeededFuture());

        // act
        service.execute(context)
                .compose(i -> service.execute(context))
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }
                    verify(client).subscribe(any());
                    verify(client).transferDataToScdTable(any());
                    verifyNoMoreInteractions(client);
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenStartLoadAndNullSysCn(VertxTestContext testContext) {
        // arrange
        val context = getRequestContext(null);
        context.getDestinationEntity().setExternalTableLocationPath(STANDALONE_SPACE_NAME);

        when(client.subscribe(any())).thenReturn(Future.succeededFuture());

        // act
        service.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }
                    verify(client).subscribe(argThat(argument -> {
                        assertThat(argument.getSpaceNames(), Matchers.contains(STANDALONE_SPACE_NAME));
                        assertEquals(UPLOAD_MESSAGE_LIMIT, argument.getMaxNumberOfMessagesPerPartition());
                        assertEquals(argument.getTopicName(), TOPIC_NAME);
                        return true;
                    }));
                    verifyNoMoreInteractions(client);
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenStartLoadX2AndNullSysCn(VertxTestContext testContext) {
        // arrange
        val context = getRequestContext(null);
        context.getDestinationEntity().setExternalTableLocationPath(STANDALONE_SPACE_NAME);

        when(client.subscribe(any())).thenReturn(Future.succeededFuture());

        // act
        service.execute(context)
                .compose(s -> service.execute(context))
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }
                    verify(client).subscribe(argThat(argument -> {
                        assertThat(argument.getSpaceNames(), Matchers.contains(STANDALONE_SPACE_NAME));
                        assertEquals(UPLOAD_MESSAGE_LIMIT, argument.getMaxNumberOfMessagesPerPartition());
                        assertEquals(argument.getTopicName(), TOPIC_NAME);
                        return true;
                    }));
                    verifyNoMoreInteractions(client);
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenStopLoadAndNullSysCn(VertxTestContext testContext) {
        // arrange
        val context = getRequestContext(null);
        context.getDestinationEntity().setExternalTableLocationPath(STANDALONE_SPACE_NAME);
        context.setLoadStart(false);

        when(client.cancelSubscription(any())).thenReturn(Future.succeededFuture());

        // act
        service.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }
                    verify(client).cancelSubscription(eq(TOPIC_NAME));
                    verifyNoMoreInteractions(client);
                }).completeNow());
    }

    private MppwRequest getRequestContext(Long sysCn) {
        return MppwKafkaRequest.builder()
                .envName("env1")
                .datamartMnemonic("test")
                .loadStart(true)
                .sysCn(sysCn)
                .sourceEntity(Entity.builder()
                        .externalTableUploadMessageLimit(UPLOAD_MESSAGE_LIMIT)
                        .build())
                .destinationEntity(Entity.builder().name("tbl1").build())
                .uploadMetadata(UploadExternalEntityMetadata.builder()
                        .name("ext_tab")
                        .externalSchema(getExternalTableSchema())
                        .uploadMessageLimit(UPLOAD_MESSAGE_LIMIT)
                        .locationPath("kafka://kafka-1.dtm.local:9092/topic")
                        .format(ExternalTableFormat.AVRO)
                        .build())
                .brokers(Collections.singletonList(new KafkaBrokerInfo("kafka.host", 9092)))
                .topic(TOPIC_NAME)
                .build();
    }

    private String getExternalTableSchema() {
        return "{\"type\":\"record\",\"name\":\"accounts\",\"namespace\":\"dm2\",\"fields\":[{\"name\":\"column1\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"column2\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"column3\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null},{\"name\":\"sys_op\",\"type\":\"int\",\"default\":0}]}";
    }

}
