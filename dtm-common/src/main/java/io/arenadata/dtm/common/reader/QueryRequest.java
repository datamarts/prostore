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
package io.arenadata.dtm.common.reader;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Query request for receiving data
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class QueryRequest {

    /**
     * Request uuid
     */
    private UUID requestId;

    /**
     * Datamart name
     */
    private String datamartMnemonic;

    /**
     * Sql query expression
     */
    private String sql;

    /**
     * attribute of prepare request
     */
    private boolean isPrepare;

    /**
     * Parameters (optional)
     */
    private QueryParameters parameters;

    public QueryRequest copy() {
        return toBuilder()
                .parameters(parameters != null ? parameters.copy() : null)
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QueryRequest)) return false;
        QueryRequest that = (QueryRequest) o;
        return requestId.equals(that.requestId) &&
                datamartMnemonic.equals(that.datamartMnemonic) &&
                sql.equals(that.sql) &&
                Objects.equals(parameters, that.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, datamartMnemonic, sql, parameters);
    }
}
