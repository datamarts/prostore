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
package ru.datamart.prostore.jdbc.core;

import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * Sql query response
 */
@Data
public class QueryResult {
    /**
     * Request identifier
     */
    private String requestId;
    /**
     * Query result List<Map<ColumnName, ColumnValue>>
     */
    private List<Map<String, Object>> result;
    /**
     * Is query result empty
     */
    private boolean empty;
    /**
     * List of system metadata
     */
    private List<ColumnMetadata> metadata;
}
