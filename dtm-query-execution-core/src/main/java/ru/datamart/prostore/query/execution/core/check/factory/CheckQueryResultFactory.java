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
package ru.datamart.prostore.query.execution.core.check.factory;

import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Component
public class CheckQueryResultFactory {

    private static final String CHECK_RESULT_COLUMN_NAME = "check_result";

    public QueryResult create(String result) {
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put(CHECK_RESULT_COLUMN_NAME, result);
        return QueryResult.builder()
                .metadata(Collections.singletonList(ColumnMetadata.builder()
                        .name(CHECK_RESULT_COLUMN_NAME)
                        .type(ColumnType.VARCHAR)
                        .build()))
                .result(Collections.singletonList(resultMap))
                .build();
    }
}
