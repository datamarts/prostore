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
package ru.datamart.prostore.common.reader;

import lombok.*;

import java.util.Objects;
import java.util.UUID;

/**
 * Input execution query request
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InputQueryRequest {

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
     * Parameters (optional)
     */
    private QueryParameters parameters;

    public InputQueryRequest copy() {
        InputQueryRequest newQueryRequest = new InputQueryRequest();
        newQueryRequest.setSql(sql);
        newQueryRequest.setDatamartMnemonic(datamartMnemonic);
        newQueryRequest.setRequestId(requestId);
        if (parameters != null) {
            newQueryRequest.setParameters(parameters.copy());
        }
        return newQueryRequest;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InputQueryRequest that = (InputQueryRequest) o;
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
