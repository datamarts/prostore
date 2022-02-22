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
package ru.datamart.prostore.query.execution.plugin.adg.base.configuration.properties;

import lombok.Data;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ToString
@ConfigurationProperties(prefix = "adg.tarantool.db")
public class TarantoolDatabaseProperties {
    String host = "localhost";
    Integer port = 3511;
    String user = "admin";
    String password = "123";
    Integer operationTimeout = 10000;
    Integer retryCount = 0;
    String engine = "MEMTX";
    Long initTimeoutMillis = 60000L;
    int vertxWorkers = 10;
}
