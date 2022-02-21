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
package ru.datamart.prostore.jdbc.core;

import ru.datamart.prostore.jdbc.util.DtmSqlException;

import java.sql.SQLException;
import java.util.Properties;

public abstract class ConnectionFactory {

    public ConnectionFactory() {
    }

    public static QueryExecutor openConnection(String host, String user, String database, String url, Properties info) throws SQLException {
            ConnectionFactory connectionFactory = new ConnectionFactoryImpl();
            QueryExecutor queryExecutor = connectionFactory.openConnectionImpl(host, user, database, url, info);
            if (queryExecutor != null) {
                return queryExecutor;
            } else {
                throw new DtmSqlException("Can't create query executor");
            }
    }

    public abstract QueryExecutor openConnectionImpl(String host, String user, String database, String url, Properties info) throws SQLException;
}
