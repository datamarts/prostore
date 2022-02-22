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
package ru.datamart.prostore.query.execution.plugin.adg.db.service;

import com.google.common.collect.Maps.EntryTransformer;
import ru.datamart.prostore.query.execution.plugin.adg.base.configuration.properties.TarantoolDatabaseProperties;
import ru.datamart.prostore.query.execution.plugin.api.exception.DataSourceException;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.tarantool.SocketChannelProvider;
import org.tarantool.TarantoolClient;
import org.tarantool.TarantoolClientConfig;
import org.tarantool.TarantoolClientImpl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
public class AdgClientProvider {
    private final TarantoolDatabaseProperties tarantoolDatabaseProperties;
    private final AtomicReference<TarantoolClient> clientReference;
    private final EntryTransformer<TarantoolClientConfig, SocketChannelProvider, TarantoolClient> clientProvider;

    @Autowired
    public AdgClientProvider(TarantoolDatabaseProperties tarantoolDatabaseProperties) {
        this.tarantoolDatabaseProperties = tarantoolDatabaseProperties;
        this.clientProvider = this::getTarantoolClient;
        this.clientReference = new AtomicReference<>(createNewClient());
    }

    protected AdgClientProvider(TarantoolDatabaseProperties tarantoolDatabaseProperties,
                                TarantoolClient tarantoolClient,
                                EntryTransformer<TarantoolClientConfig, SocketChannelProvider, TarantoolClient> clientProvider) {
        this.tarantoolDatabaseProperties = tarantoolDatabaseProperties;
        this.clientProvider = clientProvider;
        this.clientReference = new AtomicReference<>(tarantoolClient);
    }

    public TarantoolClient getClient() {
        if (!clientReference.get().isAlive()) {
            synchronized (this) {
                val deadClient = clientReference.get();
                if (!deadClient.isAlive()) {
                    try {
                        deadClient.close();
                    } catch (Exception e) {
                        log.error("Could not stop previous client", e);
                    }
                    clientReference.set(createNewClient());
                }
            }
        }

        return clientReference.get();
    }

    private TarantoolClient createNewClient() {
        TarantoolClientConfig config = new TarantoolClientConfig();
        config.username = tarantoolDatabaseProperties.getUser();
        config.password = tarantoolDatabaseProperties.getPassword();
        config.operationExpiryTimeMillis = tarantoolDatabaseProperties.getOperationTimeout();
        config.retryCount = tarantoolDatabaseProperties.getRetryCount();
        config.initTimeoutMillis = tarantoolDatabaseProperties.getInitTimeoutMillis();
        SocketChannelProvider socketChannelProvider = (i, throwable) -> {
            SocketChannel channel;
            try {
                channel = SocketChannel.open();
                channel.socket().connect(new InetSocketAddress(tarantoolDatabaseProperties.getHost(), tarantoolDatabaseProperties.getPort()));
                return channel;
            } catch (IOException e) {
                throw new DataSourceException("Error in socket provider", e);
            }
        };

        val tarantoolClient = clientProvider.transformEntry(config, socketChannelProvider);
        try {
            tarantoolClient.waitAlive(tarantoolDatabaseProperties.getInitTimeoutMillis(), TimeUnit.MILLISECONDS);
            if (!tarantoolClient.isAlive()) {
                throw new IllegalStateException("Could not start tarantool client");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting tarantool startup", e);
        }
        return tarantoolClient;
    }

    private TarantoolClient getTarantoolClient(TarantoolClientConfig config, SocketChannelProvider socketChannelProvider) {
        return new TarantoolClientImpl(socketChannelProvider, config);
    }
}
