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
package ru.datamart.prostore.query.execution.plugin.adp.base.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties("adp.datasource")
public class AdpProperties {
    private static final int DEFAULT_FETCH_SIZE = 1_000;
    private static final int DEFAULT_PREPARED_CACHE_MAX_SIZE = 256;
    private static final int DEFAULT_PREPARED_CACHE_SQL_LIMIT = 2048;

    private String user;
    private String password;
    private String host;
    private int port;
    private int poolSize;
    private int executorsCount;
    private int fetchSize = DEFAULT_FETCH_SIZE;
    private int preparedStatementsCacheMaxSize = DEFAULT_PREPARED_CACHE_MAX_SIZE;
    private int preparedStatementsCacheSqlLimit = DEFAULT_PREPARED_CACHE_SQL_LIMIT;
    private boolean preparedStatementsCache = true;
}
