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
package ru.datamart.prostore.query.execution.plugin.adqm.enrichment.service;

import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.adqm.base.factory.AdqmHelperTableNamesFactory;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.SchemaExtender;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static ru.datamart.prostore.query.execution.plugin.adqm.base.utils.Constants.*;


/**
 * Implementing a Logic to Physical Conversion
 */
@Service("adqmSchemaExtender")
public class AdqmSchemaExtender implements SchemaExtender {
    private final AdqmHelperTableNamesFactory helperTableNamesFactory;

    @Autowired
    public AdqmSchemaExtender(AdqmHelperTableNamesFactory helperTableNamesFactory) {
        this.helperTableNamesFactory = helperTableNamesFactory;
    }

    public static List<EntityField> getExtendedColumns() {
        return Arrays.asList(
            generateNewField(SYS_OP_FIELD, ColumnType.INT),
            generateNewField(SYS_TO_FIELD, ColumnType.BIGINT),
            generateNewField(SYS_FROM_FIELD, ColumnType.BIGINT),
            generateNewField(SIGN_FIELD, ColumnType.INT),
            generateNewField(SYS_CLOSE_DATE_FIELD, ColumnType.DATE)
        );
    }

    private static EntityField generateNewField(String name, ColumnType columnType) {
        return EntityField.builder()
            .type(columnType)
            .name(name)
            .build();
    }

    @Override
    public Datamart createPhysicalSchema(Datamart logicalSchema, String systemName) {
        Datamart extendedSchema = new Datamart();
        extendedSchema.setMnemonic(logicalSchema.getMnemonic());
        List<Entity> extendedEntities = new ArrayList<>();
        logicalSchema.getEntities().forEach(entity -> {
            val extendedEntity = entity.copy();
            val helperTableNames = helperTableNamesFactory.create(systemName,
                    logicalSchema.getMnemonic(),
                    extendedEntity.getName());
            extendedEntity.setSchema(helperTableNames.getSchema());
            val extendedEntityFields = new ArrayList<>(extendedEntity.getFields());
            extendedEntityFields.addAll(getExtendedColumns());
            extendedEntity.setFields(extendedEntityFields);
            extendedEntities.add(extendedEntity);
            extendedEntities.add(getExtendedSchema(extendedEntity, helperTableNames.getActual()));
            extendedEntities.add(getExtendedSchema(extendedEntity, helperTableNames.getActualShard()));
        });
        extendedEntities.stream()
            .findFirst()
            .ifPresent(datamartTable -> extendedSchema.setMnemonic(datamartTable.getSchema()));
        extendedSchema.setEntities(extendedEntities);
        return extendedSchema;
    }

    private Entity getExtendedSchema(Entity entity, String tableName) {
        return entity.toBuilder()
            .fields(entity.getFields().stream()
                .map(ef -> ef.toBuilder().build())
                .collect(Collectors.toList()))
            .name(tableName)
            .build();
    }

}
