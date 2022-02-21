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
package ru.datamart.prostore.query.execution.core.edml.dto;

import ru.datamart.prostore.common.delta.DeltaInformation;
import ru.datamart.prostore.common.metrics.RequestMetrics;
import ru.datamart.prostore.common.model.SqlProcessingType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.request.DatamartRequest;
import ru.datamart.prostore.query.execution.core.base.dto.request.CoreRequestContext;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

import static ru.datamart.prostore.common.model.SqlProcessingType.EDML;

@Getter
@Setter
@ToString
public class EdmlRequestContext extends CoreRequestContext<DatamartRequest, SqlNode> {
    private Entity sourceEntity;
    private Entity destinationEntity;
    private Long sysCn;
    private SqlNode dmlSubQuery;
    private List<Datamart> logicalSchema;
    private List<DeltaInformation> deltaInformations;

    public EdmlRequestContext(RequestMetrics metrics,
                              DatamartRequest request,
                              SqlNode sqlNode,
                              String envName) {
        super(metrics, envName, request, sqlNode);
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return EDML;
    }

}
