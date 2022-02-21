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
package ru.datamart.prostore.query.execution.core.eddl.dto;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.ExternalTableFormat;
import ru.datamart.prostore.common.plugin.exload.Type;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Download External table creation request
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class CreateDownloadExternalTableQuery extends EddlQuery {
    /**
     * Table entity
     */
    private Entity entity;

    /**
     * Type
     */
    private Type locationType;

    /**
     * Path
     */
    private String locationPath;

    /**
     * Format
     */
    private ExternalTableFormat format;

    /**
     * Avro schema in json format
     */
    private String tableSchema;

    /**
     * Chunk size
     */
    private Integer chunkSize;

    @Builder
    public CreateDownloadExternalTableQuery(String schemaName,
                                            String tableName,
                                            Entity entity,
                                            Type locationType,
                                            String locationPath,
                                            ExternalTableFormat format,
                                            String tableSchema,
                                            Integer chunkSize) {
        super(EddlAction.CREATE_DOWNLOAD_EXTERNAL_TABLE, schemaName, tableName);
        this.entity = entity;
        this.locationType = locationType;
        this.locationPath = locationPath;
        this.format = format;
        this.tableSchema = tableSchema;
        this.chunkSize = chunkSize;
    }
}
