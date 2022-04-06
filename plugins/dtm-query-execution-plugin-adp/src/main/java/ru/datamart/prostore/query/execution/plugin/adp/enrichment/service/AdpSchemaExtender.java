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
package ru.datamart.prostore.query.execution.plugin.adp.enrichment.service;

import lombok.val;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityUtils;
import ru.datamart.prostore.query.calcite.core.service.AbstractSchemaExtender;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static ru.datamart.prostore.query.execution.plugin.adp.base.Constants.*;


@Service("adpSchemaExtender")
public class AdpSchemaExtender extends AbstractSchemaExtender {

    @Override
    protected void extendLogicalEntity(Map<String, Datamart> extendedDatamarts, Entity entity, String datamartMnemonic, String systemName) {
        val logicalDatamart = getOrCreateDatamart(extendedDatamarts, datamartMnemonic);
        logicalDatamart.getEntities().add(entity);

        val extendedEntity = entity.copy();
        val extendedEntityFields = new ArrayList<>(extendedEntity.getFields());
        extendedEntityFields.addAll(getExtendedColumns());
        extendedEntity.setFields(extendedEntityFields);
        logicalDatamart.getEntities().add(EntityUtils.renameEntityAddPostfix(extendedEntity, HISTORY_TABLE_SUFFIX));
        logicalDatamart.getEntities().add(EntityUtils.renameEntityAddPostfix(extendedEntity, STAGING_TABLE_SUFFIX));
        logicalDatamart.getEntities().add(EntityUtils.renameEntityAddPostfix(extendedEntity, ACTUAL_TABLE_SUFFIX));
    }

    @Override
    protected void extendStandaloneEntity(Map<String, Datamart> extendedDatamarts, Entity entity, String datamartMnemonic) {
        val standaloneSchemaAndName = entity.getExternalTableLocationPath().split("\\.");
        if (standaloneSchemaAndName.length != 2) {
            throw new DtmException(String.format("External table path invalid. Format: [schema.tbl], actual: [%s]", entity.getExternalTableLocationPath()));
        }
        val standaloneTableSchema = standaloneSchemaAndName[0];
        val standaloneTableName = standaloneSchemaAndName[1];

        val logicalDatamart = getOrCreateDatamart(extendedDatamarts, datamartMnemonic);
        logicalDatamart.getEntities().add(entity);

        val standaloneDatamart = getOrCreateDatamart(extendedDatamarts, standaloneTableSchema);
        standaloneDatamart.getEntities().add(EntityUtils.renameEntity(entity, standaloneTableSchema, standaloneTableName));
    }

    private List<EntityField> getExtendedColumns() {
        return Arrays.asList(
                generateNewField(SYS_OP_ATTR, false),
                generateNewField(SYS_TO_ATTR, true),
                generateNewField(SYS_FROM_ATTR, false)
        );
    }

    private EntityField generateNewField(String name, boolean isNullable) {
        return EntityField.builder()
                .type(ColumnType.BIGINT)
                .name(name)
                .nullable(isNullable)
                .build();
    }

}
