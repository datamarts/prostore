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
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import lombok.*;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Modified ExecutionQueryRequest without hint
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class QuerySourceRequest {
    @NonNull
    private QueryRequest queryRequest;
    private List<Datamart> logicalSchema;
    private List<ColumnMetadata> metadata;
    private QueryTemplateResult queryTemplate;
    private SourceType sourceType;
    @NonNull
    private SqlNode query;

    public QuerySourceRequest(@NonNull QueryRequest queryRequest, @NonNull SqlNode query, SourceType sourceType) {
        this.queryRequest = queryRequest;
        this.sourceType = sourceType;
        this.query = query;
    }

    public List<Datamart> getLogicalSchema() {
        return logicalSchema.stream()
                .map(Datamart::copy)
                .collect(Collectors.toList());
    }
}
