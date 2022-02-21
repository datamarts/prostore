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
package ru.datamart.prostore.query.execution.plugin.api.mppw;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.ExternalTableLocationType;
import ru.datamart.prostore.query.execution.plugin.api.dto.PluginRequest;
import ru.datamart.prostore.query.execution.plugin.api.edml.BaseExternalEntityMetadata;
import lombok.*;

import java.util.UUID;

/**
 * Request Mppw dto
 */
@Data
@ToString
@EqualsAndHashCode(callSuper = true)
public class MppwRequest extends PluginRequest {

    protected final Entity sourceEntity;
    protected final Long sysCn;
    protected final Entity destinationEntity;
    protected final BaseExternalEntityMetadata uploadMetadata;
    protected final ExternalTableLocationType externalTableLocationType;
    /**
     * Sign of the start of mppw download
     */
    protected boolean loadStart;

    public MppwRequest(UUID requestId,
                       String envName,
                       String datamartMnemonic,
                       boolean loadStart,
                       Entity sourceEntity,
                       Long sysCn,
                       Entity destinationEntity,
                       BaseExternalEntityMetadata uploadMetadata,
                       ExternalTableLocationType externalTableLocationType) {
        super(requestId, envName, datamartMnemonic);
        this.loadStart = loadStart;
        this.sourceEntity = sourceEntity;
        this.sysCn = sysCn;
        this.destinationEntity = destinationEntity;
        this.uploadMetadata = uploadMetadata;
        this.externalTableLocationType = externalTableLocationType;
    }
}

