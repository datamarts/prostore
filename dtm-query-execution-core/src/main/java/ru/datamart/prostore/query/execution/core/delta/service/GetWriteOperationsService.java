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
package ru.datamart.prostore.query.execution.core.delta.service;

import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaWriteOp;
import ru.datamart.prostore.query.execution.core.delta.dto.query.DeltaAction;
import ru.datamart.prostore.query.execution.core.delta.dto.query.DeltaQuery;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import io.vertx.core.Future;
import lombok.val;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class GetWriteOperationsService implements DeltaService {

    public static final String SYS_CN_COLUMN = "sys_cn";
    public static final String STATUS_COLUMN = "status";
    public static final String DESTINATION_TABLE_NAME_COLUMN = "destination_table_name";
    public static final String EXTERNAL_TABLE_NAME_COLUMN = "external_table_name";
    public static final String QUERY_COLUMN = "query";
    private static final List<ColumnMetadata> METADATA = Arrays.asList(
            ColumnMetadata.builder().name(SYS_CN_COLUMN).type(ColumnType.BIGINT).build(),
            ColumnMetadata.builder().name(STATUS_COLUMN).type(ColumnType.INT).build(),
            ColumnMetadata.builder().name(DESTINATION_TABLE_NAME_COLUMN).type(ColumnType.VARCHAR).build(),
            ColumnMetadata.builder().name(EXTERNAL_TABLE_NAME_COLUMN).type(ColumnType.VARCHAR).build(),
            ColumnMetadata.builder().name(QUERY_COLUMN).type(ColumnType.VARCHAR).build()
    );

    private final DeltaServiceDao deltaServiceDao;

    public GetWriteOperationsService(ServiceDbFacade serviceDbFacade) {
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
    }

    @Override
    public Future<QueryResult> execute(DeltaQuery deltaQuery) {
        return deltaServiceDao.getDeltaWriteOperations(deltaQuery.getDatamart())
                .map(writeOps -> QueryResult.builder()
                        .metadata(METADATA)
                        .result(convertToResultSet(writeOps))
                        .build());
    }

    private List<Map<String, Object>> convertToResultSet(List<DeltaWriteOp> writeOps) {
        List<Map<String, Object>> results = new ArrayList<>();

        writeOps.forEach(op -> {
            val resultSet = new HashMap<String, Object>();
            resultSet.put(SYS_CN_COLUMN, op.getSysCn());
            resultSet.put(STATUS_COLUMN, op.getStatus());
            resultSet.put(DESTINATION_TABLE_NAME_COLUMN, op.getTableName());
            resultSet.put(EXTERNAL_TABLE_NAME_COLUMN, op.getTableNameExt());
            resultSet.put(QUERY_COLUMN, op.getQuery());

            results.add(resultSet);
        });

        return results;
    }

    @Override
    public DeltaAction getAction() {
        return DeltaAction.GET_WRITE_OPERATIONS;
    }
}
