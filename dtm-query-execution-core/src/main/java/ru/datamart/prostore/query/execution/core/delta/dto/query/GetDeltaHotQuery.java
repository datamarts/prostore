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
package ru.datamart.prostore.query.execution.core.delta.dto.query;

import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.query.execution.core.delta.dto.operation.WriteOpFinish;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.time.LocalDateTime;
import java.util.List;

import static ru.datamart.prostore.query.execution.core.delta.dto.query.DeltaAction.GET_DELTA_HOT;

@Data
@EqualsAndHashCode(callSuper = true)
public class GetDeltaHotQuery extends DeltaQuery {

    private Long cnFrom;
    private Long cnTo;
    private Long cnMax;
    private boolean isRollingBack;
    private List<WriteOpFinish> writeOpFinishList;

    @Builder
    public GetDeltaHotQuery(QueryRequest request,
                            String datamart,
                            Long deltaNum,
                            LocalDateTime deltaDate,
                            Long cnFrom,
                            Long cnTo,
                            Long cnMax,
                            boolean isRollingBack,
                            List<WriteOpFinish> writeOpFinishList) {
        super(request, datamart, deltaNum, deltaDate);
        this.cnFrom = cnFrom;
        this.cnTo = cnTo;
        this.cnMax = cnMax;
        this.isRollingBack = isRollingBack;
        this.writeOpFinishList = writeOpFinishList;
    }

    @Override
    public DeltaAction getDeltaAction() {
        return GET_DELTA_HOT;
    }
}
