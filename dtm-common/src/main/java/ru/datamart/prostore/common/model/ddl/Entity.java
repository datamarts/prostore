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
package ru.datamart.prostore.common.model.ddl;

import com.fasterxml.jackson.annotation.JsonIgnore;
import ru.datamart.prostore.common.reader.SourceType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Physical model of the service database table
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class Entity implements Serializable {

    private static final String DEFAULT_SCHEMA = "test";

    private String name;
    private String schema;
    private String viewQuery;
    private EntityType entityType;
    private ExternalTableFormat externalTableFormat;
    private String externalTableSchema;
    private ExternalTableLocationType externalTableLocationType;
    private String externalTableLocationPath;
    private Map<String, String> externalTableOptions;
    private Integer externalTableDownloadChunkSize;
    private Integer externalTableUploadMessageLimit;
    private Set<SourceType> destination;
    private Long materializedDeltaNum;
    private SourceType materializedDataSource;
    private List<EntityField> fields;

    public Entity(String nameWithSchema, List<EntityField> fields) {
        this.fields = fields;
        parseNameWithSchema(nameWithSchema);
    }

    public Entity(String name, String schema, List<EntityField> fields) {
        this.name = name;
        this.schema = schema;
        this.fields = fields;
    }

    private void parseNameWithSchema(String nameWithSchema) {
        int indexComma = nameWithSchema.indexOf(".");
        this.schema = indexComma != -1 ? nameWithSchema.substring(0, indexComma) : DEFAULT_SCHEMA;
        this.name = nameWithSchema.substring(indexComma + 1);
    }

    @JsonIgnore
    public String getNameWithSchema() {
        return schema + "." + name;
    }

    public Entity copy() {
        return toBuilder()
            .fields(fields.stream()
                .map(EntityField::copy)
                .collect(Collectors.toList()))
            .build();
    }
}

