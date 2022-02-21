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
package ru.datamart.prostore.common.reader;

import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.*;

/**
 * The result of the query execution.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueryResult {
    private UUID requestId;
    @Builder.Default
    private List<Map<String, Object>> result = new ArrayList<>();
    private List<ColumnMetadata> metadata;

    public QueryResult(UUID requestId, List<Map<String, Object>> result) {
        this.requestId = requestId;
        this.result = result;
    }

    public static QueryResult emptyResult() {
        return new EmptyQueryResult();
    }

    public boolean isEmpty() {
        return result == null || result.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryResult result1 = (QueryResult) o;
        return Objects.equals(getRequestId(), result1.getRequestId()) &&
                Objects.equals(getResult(), result1.getResult());
    }
}
