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
package ru.datamart.prostore.query.execution.plugin.api.check;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.execution.plugin.api.dto.PluginRequest;
import lombok.Builder;
import lombok.Getter;

import java.util.Set;
import java.util.UUID;

@Getter
public class CheckDataByHashInt32Request extends PluginRequest {
    private final Entity entity;
    private final Long cnFrom;
    private final Long cnTo;
    private final Set<String> columns;
    private final Long normalization;

    @Builder
    public CheckDataByHashInt32Request(
            UUID requestId,
            String envName,
            String datamart,
            Entity entity,
            Long cnFrom,
            Long cnTo,
            Set<String> columns,
            Long normalization) {
        super(requestId, envName, datamart);
        this.entity = entity;
        this.cnFrom = cnFrom;
        this.cnTo = cnTo;
        this.columns = columns;
        this.normalization = normalization;
    }
}
