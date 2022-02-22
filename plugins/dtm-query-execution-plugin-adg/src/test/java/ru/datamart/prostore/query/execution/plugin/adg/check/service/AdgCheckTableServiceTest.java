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
package ru.datamart.prostore.query.execution.plugin.adg.check.service;

import io.vertx.core.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.AdditionalMatchers;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.execution.plugin.adg.base.configuration.properties.TarantoolDatabaseProperties;
import ru.datamart.prostore.query.execution.plugin.adg.base.factory.AdgTableEntitiesFactory;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.schema.Space;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import ru.datamart.prostore.query.execution.plugin.adg.ddl.factory.AdgCreateTableQueriesFactory;
import ru.datamart.prostore.query.execution.plugin.adg.utils.TestUtils;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckTableRequest;
import ru.datamart.prostore.query.execution.plugin.api.factory.MetaTableEntityFactory;
import ru.datamart.prostore.query.execution.plugin.api.service.check.CheckTableService;

import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static ru.datamart.prostore.query.execution.plugin.adg.base.utils.ColumnFields.ACTUAL_POSTFIX;

class AdgCheckTableServiceTest {
    private static final String TEST_COLUMN_NAME = "test_column";
    private static final String NOT_TABLE_EXIST = "not_exist_table";
    private static final String ENV = "env";
    private Entity entity;
    private CheckTableRequest checkTableRequest;
    private CheckTableService adgCheckTableService;

    @BeforeEach
    void setUp() {

        AdgCartridgeClient adgClient = mock(AdgCartridgeClient.class);
        entity = TestUtils.getEntity();
        int fieldsCount = entity.getFields().size();
        entity.getFields().add(EntityField.builder()
                .name(TEST_COLUMN_NAME)
                .ordinalPosition(fieldsCount + 1)
                .type(ColumnType.BIGINT)
                .nullable(true)
                .build());
        checkTableRequest = new CheckTableRequest(UUID.randomUUID(), ENV, entity.getSchema(), entity);

        Map<String, Space> spaces = TestUtils.getSpaces(entity);
        when(adgClient.getSpaceDescriptions(spaces.keySet()))
                .thenReturn(Future.succeededFuture(spaces));
        when(adgClient.getSpaceDescriptions(AdditionalMatchers.not(eq(spaces.keySet()))))
                .thenReturn(Future.failedFuture(String.format(CheckTableService.TABLE_NOT_EXIST_ERROR_TEMPLATE,
                        NOT_TABLE_EXIST + ACTUAL_POSTFIX)));

        adgCheckTableService = new AdgCheckTableService(adgClient,
                new AdgCreateTableQueriesFactory(new AdgTableEntitiesFactory(new TarantoolDatabaseProperties())));
    }

    @Test
    void testSuccess() {
        assertTrue(adgCheckTableService.check(checkTableRequest).succeeded());
    }

    @Test
    void testTableNotExist() {
        entity.setName("not_exist_table");
        assertThat(adgCheckTableService.check(checkTableRequest).cause().getMessage(),
                containsString(String.format(CheckTableService.TABLE_NOT_EXIST_ERROR_TEMPLATE,
                        NOT_TABLE_EXIST + ACTUAL_POSTFIX)));
    }

    @Test
    void testColumnNotExist() {
        entity.getFields().add(EntityField.builder()
                .name("not_exist_column")
                .size(1)
                .type(ColumnType.VARCHAR)
                .nullable(true)
                .build());
        String expectedError = String.format(AdgCheckTableService.COLUMN_NOT_EXIST_ERROR_TEMPLATE,
                "not_exist_column");
        assertThat(adgCheckTableService.check(checkTableRequest).cause().getMessage(),
                containsString(expectedError));
    }

    @Test
    void testDataType() {
        String expectedError = String.format(CheckTableService.FIELD_ERROR_TEMPLATE,
                MetaTableEntityFactory.DATA_TYPE, "string", "integer");
        testColumns(field -> field.setType(ColumnType.VARCHAR), expectedError);

    }

    private void testColumns(Consumer<EntityField> consumer,
                             String expectedError) {
        EntityField testColumn = entity.getFields().stream()
                .filter(field -> TEST_COLUMN_NAME.equals(field.getName()))
                .findAny()
                .orElseThrow(RuntimeException::new);
        consumer.accept(testColumn);
        assertThat(adgCheckTableService.check(checkTableRequest).cause().getMessage(),
                containsString(expectedError));
    }
}
