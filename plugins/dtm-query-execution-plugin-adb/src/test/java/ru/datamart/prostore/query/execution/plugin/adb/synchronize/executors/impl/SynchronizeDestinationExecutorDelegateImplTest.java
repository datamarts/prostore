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
package ru.datamart.prostore.query.execution.plugin.adb.synchronize.executors.impl;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.plugin.adb.synchronize.executors.SynchronizeDestinationExecutor;
import ru.datamart.prostore.query.execution.plugin.adb.synchronize.executors.SynchronizeDestinationExecutorDelegate;
import ru.datamart.prostore.query.execution.plugin.api.exception.SynchronizeDatasourceException;
import ru.datamart.prostore.query.execution.plugin.api.synchronize.SynchronizeRequest;
import io.vertx.core.Future;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SynchronizeDestinationExecutorDelegateImplTest {
    private static final SynchronizeRequest REQUEST = new SynchronizeRequest(UUID.randomUUID(), "dev", "test", null, Entity.builder().build(),
            null, null, null);

    @Mock
    private SynchronizeDestinationExecutor executor1;

    @Mock
    private SynchronizeDestinationExecutor executor2;

    @Test
    void shouldThrowWhenTwoSameExecutors() {
        // arrange
        when(executor1.getDestination()).thenReturn(SourceType.ADB);
        when(executor2.getDestination()).thenReturn(SourceType.ADB);

        // act assert
        assertThrows(IllegalArgumentException.class, () -> new SynchronizeDestinationExecutorDelegateImpl(Arrays.asList(executor1, executor2)));
    }

    @Test
    void shouldSuccessWhenAllExecutorsPresent() {
        // arrange
        when(executor1.getDestination()).thenReturn(SourceType.ADB);
        when(executor1.execute(Mockito.any())).thenReturn(Future.succeededFuture(1L));
        when(executor2.getDestination()).thenReturn(SourceType.ADG);
        when(executor2.execute(Mockito.any())).thenReturn(Future.succeededFuture(2L));


        SynchronizeDestinationExecutorDelegate delegate = new SynchronizeDestinationExecutorDelegateImpl(Arrays.asList(executor1, executor2));

        // act
        Future<Long> result1 = delegate.execute(SourceType.ADB, REQUEST);
        Future<Long> result2 = delegate.execute(SourceType.ADG, REQUEST);

        //assert
        assertNotNull(result1);
        assertTrue(result1.succeeded());
        assertEquals(1L, result1.result());

        assertNotNull(result2);
        assertTrue(result2.succeeded());
        assertEquals(2L, result2.result());
    }

    @Test
    void shouldThrowWhenUnknownExecutor() {
        // arrange
        when(executor1.getDestination()).thenReturn(SourceType.ADB);

        SynchronizeDestinationExecutorDelegate delegate = new SynchronizeDestinationExecutorDelegateImpl(Collections.singletonList(executor1));

        // act
        Future<Long> result = delegate.execute(SourceType.ADG, REQUEST);

        // assert
        assertNotNull(result);
        assertTrue(result.failed());
        assertEquals(SynchronizeDatasourceException.class, result.cause().getClass());
        assertTrue(result.cause().getMessage().contains("Synchronize[ADB->ADG] is not implemented"));
    }

}