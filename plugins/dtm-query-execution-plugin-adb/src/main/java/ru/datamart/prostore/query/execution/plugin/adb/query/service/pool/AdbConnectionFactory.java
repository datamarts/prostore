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
package ru.datamart.prostore.query.execution.plugin.adb.query.service.pool;

import ru.datamart.prostore.query.execution.plugin.adb.base.configuration.properties.AdbProperties;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgConnection;
import lombok.val;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class AdbConnectionFactory {
    private final AdbProperties adbProperties;
    private final PgConnectOptions connectOptions;

    public AdbConnectionFactory(AdbProperties adbProperties,
                                @Value("${core.env.name}") String database) {
        this.adbProperties = adbProperties;
        val pgConnectOptions = new PgConnectOptions();
        pgConnectOptions.setDatabase(database);
        pgConnectOptions.setHost(adbProperties.getHost());
        pgConnectOptions.setPort(adbProperties.getPort());
        pgConnectOptions.setUser(adbProperties.getUser());
        pgConnectOptions.setPassword(adbProperties.getPassword());
        pgConnectOptions.setPreparedStatementCacheMaxSize(adbProperties.getPreparedStatementsCacheMaxSize());
        pgConnectOptions.setPreparedStatementCacheSqlLimit(adbProperties.getPreparedStatementsCacheSqlLimit());
        pgConnectOptions.setCachePreparedStatements(adbProperties.isPreparedStatementsCache());
        pgConnectOptions.setPipeliningLimit(1);
        this.connectOptions = pgConnectOptions;
    }

    public Future<PgConnection> createPgConnection(Vertx vertx) {
        return PgConnection.connect(vertx, connectOptions);
    }

    public AdbConnection createAdbConnection(Vertx vertx) {
        return new AdbConnection(this, vertx, adbProperties);
    }
}
