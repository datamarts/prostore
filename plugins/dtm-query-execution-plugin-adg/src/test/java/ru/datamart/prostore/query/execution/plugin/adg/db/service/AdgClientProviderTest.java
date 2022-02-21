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
package ru.datamart.prostore.query.execution.plugin.adg.db.service;

import ru.datamart.prostore.query.execution.plugin.adg.base.configuration.properties.TarantoolDatabaseProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.tarantool.TarantoolClient;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AdgClientProviderTest {

    @Mock
    private TarantoolDatabaseProperties tarantoolDatabaseProperties;

    @Mock
    private TarantoolClient tarantoolClient1;

    @Mock
    private TarantoolClient tarantoolClient2;

    private AdgClientProvider adgClientProvider;

    @BeforeEach
    void setUp() {
        adgClientProvider = new AdgClientProvider(tarantoolDatabaseProperties, tarantoolClient1, (tarantoolClientConfig, socketChannelProvider) -> tarantoolClient2);

        lenient().when(tarantoolDatabaseProperties.getUser()).thenReturn("USER");
        lenient().when(tarantoolDatabaseProperties.getPassword()).thenReturn("PASS");
        lenient().when(tarantoolDatabaseProperties.getOperationTimeout()).thenReturn(1000);
        lenient().when(tarantoolDatabaseProperties.getRetryCount()).thenReturn(0);
        lenient().when(tarantoolDatabaseProperties.getInitTimeoutMillis()).thenReturn(60000L);
        lenient().when(tarantoolDatabaseProperties.getHost()).thenReturn("HOST");
        lenient().when(tarantoolDatabaseProperties.getPort()).thenReturn(1234);
        lenient().when(tarantoolDatabaseProperties.getEngine()).thenReturn("ENGINE");
    }

    @Test
    void shouldReturnSameClientWhenAlive() {
        // arrange
        when(tarantoolClient1.isAlive()).thenReturn(true);

        // act
        TarantoolClient client = adgClientProvider.getClient();

        // assert
        assertSame(tarantoolClient1, client);
    }

    @Test
    void shouldReturnSameClientWhenWasDeadButBeingOk() {
        // arrange
        when(tarantoolClient1.isAlive()).thenReturn(false).thenReturn(true);

        // act
        TarantoolClient client = adgClientProvider.getClient();

        // assert
        assertSame(tarantoolClient1, client);
    }

    @Test
    void shouldReturnCloseAndCreateNewClientWhenClientDead() throws InterruptedException {
        // arrange
        when(tarantoolClient1.isAlive()).thenReturn(false);
        when(tarantoolClient2.isAlive()).thenReturn(true);

        // act
        TarantoolClient client = adgClientProvider.getClient();

        // assert
        verify(tarantoolClient1).close();
        verify(tarantoolClient2).waitAlive(eq(tarantoolDatabaseProperties.getInitTimeoutMillis()), eq(TimeUnit.MILLISECONDS));
        verify(tarantoolClient2).isAlive();
        assertSame(tarantoolClient2, client);
    }

    @Test
    void shouldThrowWhenWaitInterrupted() throws InterruptedException {
        // arrange
        when(tarantoolClient1.isAlive()).thenReturn(false);
        when(tarantoolClient2.waitAlive(anyLong(), any())).thenThrow(new InterruptedException("Interrupted"));

        // act
        assertThrows(IllegalStateException.class, () -> adgClientProvider.getClient());
        verify(tarantoolClient1).close();
    }

    @Test
    void shouldThrowWhenNotAliveAfterCreate() {
        // arrange
        when(tarantoolClient1.isAlive()).thenReturn(false);
        when(tarantoolClient2.isAlive()).thenReturn(false);

        // act
        assertThrows(IllegalStateException.class, () -> adgClientProvider.getClient());
        verify(tarantoolClient1).close();
    }

    @Test
    void shouldNotThrowWhenExceptionOnClose() throws InterruptedException {
        // arrange
        when(tarantoolClient1.isAlive()).thenReturn(false);
        when(tarantoolClient2.isAlive()).thenReturn(true);
        doThrow(new IllegalStateException("Exception")).when(tarantoolClient1).close();

        // act
        TarantoolClient client = adgClientProvider.getClient();

        // assert
        verify(tarantoolClient1).close();
        verify(tarantoolClient2).waitAlive(eq(tarantoolDatabaseProperties.getInitTimeoutMillis()), eq(TimeUnit.MILLISECONDS));
        verify(tarantoolClient2).isAlive();
        assertSame(tarantoolClient2, client);
    }

}