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


import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.UUID;

/**
 * Sql query request
 */
@Data
@RequiredArgsConstructor
@NoArgsConstructor
public class QueryRequest {
    /**
     * Request UUID
     */
    private UUID requestId;
    /**
     * Datamart
     */
    @NonNull
    private String datamartMnemonic;
    /**
     * sql query
     */
    @NonNull
    private String sql;
    /**
     * query parameters
     */
    private QueryParameters parameters;

    public QueryRequest(UUID requestId,
                        @NonNull String datamartMnemonic,
                        @NonNull String sql,
                        QueryParameters parameters) {
        this.requestId = requestId;
        this.datamartMnemonic = datamartMnemonic;
        this.sql = sql;
        this.parameters = parameters;
    }
}
