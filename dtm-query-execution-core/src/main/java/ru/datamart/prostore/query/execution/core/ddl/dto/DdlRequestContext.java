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
package ru.datamart.prostore.query.execution.core.ddl.dto;

import ru.datamart.prostore.common.metrics.RequestMetrics;
import ru.datamart.prostore.common.model.SqlProcessingType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.post.PostSqlActionType;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.common.request.DatamartRequest;
import ru.datamart.prostore.query.execution.core.base.dto.request.CoreRequestContext;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.List;

import static ru.datamart.prostore.common.model.SqlProcessingType.DDL;
import static ru.datamart.prostore.query.execution.core.ddl.dto.DdlType.UNKNOWN;

@Getter
@Setter
public class DdlRequestContext extends CoreRequestContext<DatamartRequest, SqlNode> {

    private DdlType ddlType;
    private SqlCall sqlCall;
    private String datamartName;
    private Entity entity;
    private SourceType sourceType;
    private List<PostSqlActionType> postActions;

    public DdlRequestContext(RequestMetrics metrics,
                             DatamartRequest request,
                             SqlNode query,
                             SourceType sourceType,
                             String envName) {
        super(metrics, envName, request, query);
        this.ddlType = UNKNOWN;
        this.sourceType = sourceType;
        this.postActions = new ArrayList<>();
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return DDL;
    }

}
