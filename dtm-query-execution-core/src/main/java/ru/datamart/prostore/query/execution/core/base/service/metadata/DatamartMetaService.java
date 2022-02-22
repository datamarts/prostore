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
package ru.datamart.prostore.query.execution.core.base.service.metadata;

import ru.datamart.prostore.query.execution.core.base.dto.metadata.DatamartEntity;
import ru.datamart.prostore.query.execution.core.base.dto.metadata.DatamartInfo;
import ru.datamart.prostore.query.execution.core.base.dto.metadata.EntityAttribute;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.DatamartDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class DatamartMetaService {

    private final DatamartDao datamartDao;
    private final EntityDao entityDao;

    public DatamartMetaService(ServiceDbFacade serviceDbFacade) {
        this.datamartDao = serviceDbFacade.getServiceDbDao().getDatamartDao();
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
    }

    public Future<List<DatamartInfo>> getDatamartMeta() {
        return datamartDao.getDatamartMeta();
    }

    public Future<List<DatamartEntity>> getEntitiesMeta(String datamartMnemonic) {
        return entityDao.getEntitiesMeta(datamartMnemonic);
    }

    public Future<List<EntityAttribute>> getAttributesMeta(String datamartMnemonic,
                                                           String entityMnemonic) {
        return entityDao.getEntity(datamartMnemonic, entityMnemonic)
                .map(entity -> entity.getFields().stream()
                        .map(ef -> EntityAttribute.builder()
                                .datamartMnemonic(datamartMnemonic)
                                .entityMnemonic(entityMnemonic)
                                .accuracy(ef.getAccuracy())
                                .distributeKeykOrder(ef.getShardingOrder())
                                .primaryKeyOrder(ef.getPrimaryOrder())
                                .dataType(ef.getType())
                                .length(ef.getSize())
                                .mnemonic(ef.getName())
                                .ordinalPosition(ef.getOrdinalPosition())
                                .nullable(ef.getNullable())
                                .accuracy(ef.getAccuracy())
                                .build())
                        .collect(Collectors.toList()));
    }
}
