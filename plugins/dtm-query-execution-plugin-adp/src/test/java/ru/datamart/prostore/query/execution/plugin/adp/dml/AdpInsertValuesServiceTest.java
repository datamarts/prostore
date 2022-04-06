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
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.query.execution.plugin.adp.dml.insert.values.AdpLogicalInsertValuesService;
import ru.datamart.prostore.query.execution.plugin.adp.dml.insert.values.AdpStandaloneInsertValuesService;
import ru.datamart.prostore.query.execution.plugin.api.request.InsertValuesRequest;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AdpInsertValuesServiceTest {
    @Mock
    private AdpLogicalInsertValuesService logicalService;

    @Mock
    private AdpStandaloneInsertValuesService standaloneService;

    @InjectMocks
    private AdpInsertValuesService service;

    @Test
    void shouldSuccessWhenWritableType() {
        // arrange
        when(standaloneService.execute(any())).thenReturn(Future.succeededFuture());
        val entity = getEntity(EntityType.WRITEABLE_EXTERNAL_TABLE);

        // act
        val result = service.execute(getInsertRequest(entity));

        // assert
        assertTrue(result.succeeded());
        verify(standaloneService, times(1)).execute(any());
    }

    @Test
    void shouldSuccessWhenOtherType() {
        // arrange
        when(logicalService.execute(any())).thenReturn(Future.succeededFuture());
        val entity = getEntity(EntityType.TABLE);

        // act
        val result = service.execute(getInsertRequest(entity));

        // assert
        assertTrue(result.succeeded());
        verify(logicalService, times(1)).execute(any());
    }

    private InsertValuesRequest getInsertRequest(Entity entity) {
        return new InsertValuesRequest(UUID.randomUUID(), "dev", "datamart",
                1L, entity, null, null);
    }

    private Entity getEntity(EntityType type) {
        return Entity.builder()
                .entityType(type)
                .build();
    }

}