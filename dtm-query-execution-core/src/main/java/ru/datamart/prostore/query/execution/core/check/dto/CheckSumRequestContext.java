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
package ru.datamart.prostore.query.execution.core.check.dto;

import lombok.Builder;
import lombok.Data;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.calcite.core.extension.check.CheckSumType;

import java.util.Set;

@Data
@Builder
public class CheckSumRequestContext {
    private CheckContext checkContext;
    private String datamart;
    private Long deltaNum;
    private Long normalization;
    private long cnFrom;
    private long cnTo;
    private Entity entity;
    private Set<String> columns;
    private CheckSumType checksumType;

    public CheckSumRequestContext copy() {
        return CheckSumRequestContext.builder()
                .checkContext(checkContext)
                .datamart(datamart)
                .normalization(normalization)
                .deltaNum(deltaNum)
                .cnFrom(cnFrom)
                .cnTo(cnTo)
                .entity(entity)
                .columns(columns)
                .checksumType(checksumType)
                .build();
    }
}
