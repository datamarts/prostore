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
package ru.datamart.prostore.query.execution.core.rollback.factory;

import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.execution.core.edml.dto.EraseWriteOpResult;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class RollbackWriteOpsQueryResultFactory {

    public static final String TABLE_NAME_COLUMN = "table_name";
    public static final String SYS_CN_OPS_COLUMN = "sys_cn_operations";

    public QueryResult create(List<EraseWriteOpResult> eraseOps) {
        List<Map<String, Object>> eraseResult = eraseOps.stream()
                .collect(Collectors.groupingBy(EraseWriteOpResult::getTableName,
                        Collectors.mapping(er -> String.valueOf(er.getSysCn()), Collectors.joining(", "))))
                .entrySet().stream().map(er -> {
                    Map<String, Object> resultMap = new HashMap<>();
                    resultMap.put(TABLE_NAME_COLUMN, er.getKey());
                    resultMap.put(SYS_CN_OPS_COLUMN, er.getValue());
                    return resultMap;
                }).collect(Collectors.toList());
        return QueryResult.builder()
                .metadata(Arrays.asList(ColumnMetadata.builder()
                                .name(TABLE_NAME_COLUMN)
                                .type(ColumnType.VARCHAR)
                                .build(),
                        ColumnMetadata.builder()
                                .name(SYS_CN_OPS_COLUMN)
                                .type(ColumnType.VARCHAR)
                                .build()))
                .result(eraseResult)
                .build();
    }
}
