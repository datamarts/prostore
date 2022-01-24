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
package io.arenadata.dtm.query.execution.core.ddl.service.impl.table;

import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.OperationNames;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlCreateTable;
import io.arenadata.dtm.query.execution.core.base.exception.datamart.DatamartNotExistsException;
import io.arenadata.dtm.query.execution.core.base.exception.entity.EntityAlreadyExistsException;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.SetEntityState;
import io.arenadata.dtm.query.execution.core.base.service.metadata.MetadataCalciteGenerator;
import io.arenadata.dtm.query.execution.core.base.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlType;
import io.arenadata.dtm.query.execution.core.ddl.service.QueryResultDdlExecutor;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

import static io.arenadata.dtm.query.execution.core.ddl.utils.ValidationUtils.*;

@Slf4j
@Component
public class CreateTableExecutor extends QueryResultDdlExecutor {

    private final MetadataCalciteGenerator metadataCalciteGenerator;
    private final DatamartDao datamartDao;
    private final EntityDao entityDao;
    private final DataSourcePluginService dataSourcePluginService;

    @Autowired
    public CreateTableExecutor(MetadataExecutor metadataExecutor,
                               ServiceDbFacade serviceDbFacade,
                               @Qualifier("coreSqlDialect") SqlDialect sqlDialect,
                               MetadataCalciteGenerator metadataCalciteGenerator,
                               DataSourcePluginService dataSourcePluginService) {
        super(metadataExecutor, serviceDbFacade, sqlDialect);
        this.metadataCalciteGenerator = metadataCalciteGenerator;
        datamartDao = serviceDbFacade.getServiceDbDao().getDatamartDao();
        entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
        this.dataSourcePluginService = dataSourcePluginService;
    }

    @Override
    public Future<QueryResult> execute(DdlRequestContext context, String sqlNodeName) {
        return Future.future(promise -> {
            val datamartName = getSchemaName(context.getDatamartName(), sqlNodeName);
            context.getRequest().getQueryRequest().setDatamartMnemonic(datamartName);
            context.setDdlType(DdlType.CREATE_TABLE);
            SqlCreateTable sqlCreate = (SqlCreateTable) context.getSqlNode();
            val changeQuery = sqlNodeToString(sqlCreate);
            val entity = metadataCalciteGenerator.generateTableMetadata(sqlCreate);
            entity.setEntityType(EntityType.TABLE);
            val requestDestination = ((SqlCreateTable) context.getSqlNode()).getDestination().getDatasourceTypes();
            val destination = Optional.ofNullable(requestDestination)
                    .orElse(dataSourcePluginService.getSourceTypes());
            entity.setDestination(destination);
            validateFields(entity.getFields());
            context.setEntity(entity);
            context.setDatamartName(datamartName);
            datamartDao.existsDatamart(datamartName)
                    .compose(isExistsDatamart -> isExistsDatamart ?
                            entityDao.existsEntity(datamartName, entity.getName()) : getNotExistsDatamartFuture(datamartName))
                    .compose(isExistsEntity -> isExistsEntity ?
                            getEntityAlreadyExistsFuture(entity.getNameWithSchema()) :
                            writeNewChangelogRecord(datamartName, entity.getName(), changeQuery)
                                    .compose(deltaOk -> executeRequest(context)
                                            .compose(v -> entityDao.setEntityState(entity, deltaOk, changeQuery, SetEntityState.CREATE))))
                    .onSuccess(success -> {
                        log.debug("Table [{}] in datamart [{}] successfully created",
                                context.getEntity().getName(),
                                context.getDatamartName());
                        promise.complete(QueryResult.emptyResult());
                    })
                    .onFailure(promise::fail);
        });
    }

    private void validateFields(List<EntityField> fields) {
        checkRequiredKeys(fields);
        checkShardingKeys(fields);
        checkCharFieldsSize(fields);
        checkFieldsDuplication(fields);
    }

    private Future<Void> getEntityAlreadyExistsFuture(String entityNameWithSchema) {
        return Future.failedFuture(new EntityAlreadyExistsException(entityNameWithSchema));
    }

    private Future<Boolean> getNotExistsDatamartFuture(String datamartName) {
        return Future.failedFuture(new DatamartNotExistsException(datamartName));
    }

    @Override
    public String getOperationKind() {
        return OperationNames.CREATE_TABLE;
    }
}
