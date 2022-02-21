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
package ru.datamart.prostore.query.execution.plugin.adqm.query.service;

import ru.datamart.prostore.common.reader.QueryParameters;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import io.vertx.core.Future;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Query execution service
 */
public interface DatabaseExecutor {

    Future<List<Map<String, Object>>> execute(String sql, List<ColumnMetadata> metadata);

    Future<Void> executeUpdate(String sql);

    Future<List<Map<String, Object>>> executeWithParams(String sql, QueryParameters queryParameters, List<ColumnMetadata> metadata);

    default Future<List<Map<String, Object>>> execute(String sql) {
        return execute(sql, Collections.emptyList());
    }
}
