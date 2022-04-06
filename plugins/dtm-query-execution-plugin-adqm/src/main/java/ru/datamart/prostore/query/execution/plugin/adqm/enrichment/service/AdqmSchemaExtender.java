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

import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityUtils;
import ru.datamart.prostore.query.calcite.core.service.AbstractSchemaExtender;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.adqm.base.factory.AdqmHelperTableNamesFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static ru.datamart.prostore.query.execution.plugin.adqm.base.utils.Constants.*;


/**
 * Implementing a Logic to Physical Conversion
 */
@Service("adqmSchemaExtender")
public class AdqmSchemaExtender extends AbstractSchemaExtender {
    private static final String SHARD_POSTFIX = "_shard";
    private final AdqmHelperTableNamesFactory helperTableNamesFactory;

    @Autowired
    public AdqmSchemaExtender(AdqmHelperTableNamesFactory helperTableNamesFactory) {
        this.helperTableNamesFactory = helperTableNamesFactory;
    }

    @Override
    protected void extendLogicalEntity(Map<String, Datamart> extendedDatamarts, Entity entity, String datamartMnemonic, String systemName) {
        val logicalDatamart = getOrCreateDatamart(extendedDatamarts, datamartMnemonic);
        logicalDatamart.getEntities().add(entity);

        val extendedEntity = entity.copy();
        val helperTableNames = helperTableNamesFactory.create(systemName,
                datamartMnemonic,
                extendedEntity.getName());
        extendedEntity.setSchema(helperTableNames.getSchema());
        val extendedEntityFields = new ArrayList<>(extendedEntity.getFields());
        extendedEntityFields.addAll(getExtendedColumns());
        extendedEntity.setFields(extendedEntityFields);
        val physicalDatamart = getOrCreateDatamart(extendedDatamarts, helperTableNames.getSchema());
        physicalDatamart.getEntities().add(EntityUtils.renameEntity(extendedEntity, helperTableNames.getSchema(), helperTableNames.getActual()));
        physicalDatamart.getEntities().add(EntityUtils.renameEntity(extendedEntity, helperTableNames.getSchema(), helperTableNames.getActualShard()));
    }

    @Override
    protected void extendStandaloneEntity(Map<String, Datamart> extendedDatamarts, Entity entity, String datamartMnemonic) {
        val logicalDatamart = getOrCreateDatamart(extendedDatamarts, datamartMnemonic);
        logicalDatamart.getEntities().add(entity);

        val standaloneSchemaAndName = entity.getExternalTableLocationPath().split("\\.");
        if (standaloneSchemaAndName.length != 2) {
            throw new DtmException(String.format("External table path invalid. Format: [schema.tbl], actual: [%s]", entity.getExternalTableLocationPath()));
        }
        val standaloneTableSchema = standaloneSchemaAndName[0];
        val standaloneTableName = standaloneSchemaAndName[1];

        val physicalDatamart = getOrCreateDatamart(extendedDatamarts, standaloneTableSchema);
        physicalDatamart.getEntities().add(EntityUtils.renameEntity(entity, standaloneTableSchema, standaloneTableName));
        physicalDatamart.getEntities().add(EntityUtils.renameEntity(entity, standaloneTableSchema, standaloneTableName + SHARD_POSTFIX));
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
}
