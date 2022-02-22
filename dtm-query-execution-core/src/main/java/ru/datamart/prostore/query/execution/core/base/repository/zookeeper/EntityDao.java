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
package ru.datamart.prostore.query.execution.core.base.repository.zookeeper;

import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Repository;
import ru.datamart.prostore.common.configuration.core.CoreConstants;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Changelog;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.execution.core.base.configuration.CacheConfiguration;
import ru.datamart.prostore.query.execution.core.base.dto.metadata.DatamartEntity;
import ru.datamart.prostore.query.execution.core.base.exception.datamart.DatamartNotExistsException;
import ru.datamart.prostore.query.execution.core.base.exception.entity.EntityAlreadyExistsException;
import ru.datamart.prostore.query.execution.core.base.exception.entity.EntityNotExistsException;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import ru.datamart.prostore.query.execution.core.delta.dto.OkDelta;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static ru.datamart.prostore.serialization.CoreSerialization.deserialize;
import static ru.datamart.prostore.serialization.CoreSerialization.serialize;

@Slf4j
@Repository
public class EntityDao implements ZookeeperDao<Entity> {
    public static final String SEQUENCE_NUMBER_TEMPLATE = "0000000000";
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final String PREVIOUS_NOT_COMPLETED = "Previous change operation is not completed in datamart [%s]";
    private final ZookeeperExecutor executor;
    private final String envPath;

    public EntityDao(ZookeeperExecutor executor, @Value("${core.env.name}") String systemName) {
        this.executor = executor;
        envPath = "/" + systemName;
    }

    public Future<List<DatamartEntity>> getEntitiesMeta(String datamartMnemonic) {
        //TODO implemented receiving entity column informations
        return getEntityNamesByDatamart(datamartMnemonic)
                .map(names -> names.stream()
                        .filter(name -> !name.startsWith("logic_schema_"))
                        .map(name -> new DatamartEntity(null, name, datamartMnemonic))
                        .collect(Collectors.toList()));
    }

    @CacheEvict(
            value = CacheConfiguration.ENTITY_CACHE,
            key = "new ru.datamart.prostore.query.execution.core.base.dto.cache.EntityKey(#entity.getSchema(), #entity.getName())"
    )
    public Future<Void> setEntityState(Entity entity, OkDelta deltaOk, String changeQuery, SetEntityState state) {
        val changelogPath = getChangelogPath(entity.getSchema());
        val changelogStat = new Stat();
        val deltaNum = deltaOk == null ? null : deltaOk.getDeltaNum();
        return executor.getData(changelogPath, null, changelogStat)
                .compose(bytes -> checkChangelog(bytes, entity.getSchema(), changeQuery, deltaNum))
                .compose(changelog -> executor.getChildren(changelogPath)
                        .compose(children -> applySetEntityState(changelog, changelogPath, entity, state, changelogStat, children.size())));
    }

    private Future<Void> applySetEntityState(Changelog changelog, String changelogPath, Entity entity,
                                             SetEntityState state, Stat changelogStat, int operationNumber) {
        changelog.setDateTimeEnd(LocalDateTime.now(CoreConstants.CORE_ZONE_ID).format(DATE_TIME_FORMATTER));

        val ops = new ArrayList<Op>();
        switch (state) {
            case CREATE:
                ops.add(Op.create(getTargetPath(entity), serialize(entity), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
                break;
            case DELETE:
                ops.add(Op.delete(getTargetPath(entity), -1));
                break;
            case UPDATE:
                ops.add(Op.setData(getTargetPath(entity), serialize(entity), -1));
                break;
            default:
                throw new DtmException(String.format("Unknown SetEntityState [%s]", state));
        }
        ops.addAll(commonSetEntityStateOps(changelog, entity.getSchema(), changelogPath, changelogStat, operationNumber));

        return executor.multi(ops)
                .<Void>mapEmpty()
                .otherwise(error -> {
                    if (error instanceof KeeperException.NodeExistsException) {
                        throw warn(new EntityAlreadyExistsException(entity.getNameWithSchema(), error));
                    } else if (error instanceof KeeperException.BadVersionException) {
                        throw warn(new DtmException(String.format(PREVIOUS_NOT_COMPLETED, entity.getSchema())));
                    }

                    throw warn(new DtmException(String.format("Can't %s entity [%s]", state, entity.getNameWithSchema()), error));
                });
    }

    private List<Op> commonSetEntityStateOps(Changelog changelog, String datamart, String changelogPath, Stat changelogStat, int opNum) {
        return Arrays.asList(
                Op.setData(changelogPath, null, changelogStat.getVersion()),
                Op.delete(getBlockPath(datamart), -1),
                Op.delete(getImmutablePath(datamart), -1),
                Op.create(changelogPath + "/" + toSequenceNumber(opNum), serialize(changelog), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
        );
    }

    private Future<Changelog> checkChangelog(byte[] bytes, String datamart, String changeQuery, Long deltaNum) {
        if (bytes == null) {
            throw new DtmException(String.format("No changelog data in [%s]", datamart));
        }

        val changelog = deserialize(bytes, Changelog.class);
        if (!Objects.equals(changeQuery, changelog.getChangeQuery()) || !Objects.equals(deltaNum, changelog.getDeltaNum())) {
            throw new DtmException(String.format(PREVIOUS_NOT_COMPLETED, datamart));
        }

        return Future.succeededFuture(changelog);
    }

    @CacheEvict(
            value = CacheConfiguration.ENTITY_CACHE,
            key = "new ru.datamart.prostore.query.execution.core.base.dto.cache.EntityKey(#entity.getSchema(), #entity.getName())"
    )
    public Future<Void> createEntity(Entity entity) {
        byte[] entityData = serialize(entity);
        return executor.createPersistentPath(getTargetPath(entity), entityData)
                .<Void>mapEmpty()
                .otherwise(error -> {
                    if (error instanceof KeeperException.NoNodeException) {
                        throw new DatamartNotExistsException(entity.getSchema(), error);
                    } else if (error instanceof KeeperException.NodeExistsException) {
                        throw warn(new EntityAlreadyExistsException(entity.getNameWithSchema(), error));
                    } else {
                        throw new DtmException(String.format("Can't create entity [%s]", entity.getNameWithSchema()), error);
                    }
                });
    }

    @CacheEvict(
            value = CacheConfiguration.ENTITY_CACHE,
            key = "new ru.datamart.prostore.query.execution.core.base.dto.cache.EntityKey(#entity.getSchema(), #entity.getName())"
    )
    public Future<Void> updateEntity(Entity entity) {
        byte[] entityData = serialize(entity);
        return executor.setData(getTargetPath(entity), entityData, -1)
                .<Void>mapEmpty()
                .otherwise(error -> {
                    if (error instanceof KeeperException.NoNodeException) {
                        throw warn(new EntityNotExistsException(entity.getNameWithSchema()));
                    } else {
                        throw new DtmException(String.format("Can't update entity [%s]",
                                entity.getNameWithSchema()),
                                error);
                    }
                });
    }

    public Future<Boolean> existsEntity(String datamartMnemonic, String entityName) {
        return executor.exists(getTargetPath(datamartMnemonic, entityName));
    }

    @CacheEvict(
            value = CacheConfiguration.ENTITY_CACHE,
            key = "new ru.datamart.prostore.query.execution.core.base.dto.cache.EntityKey(#datamartMnemonic, #entityName)"
    )
    public Future<Void> deleteEntity(String datamartMnemonic, String entityName) {
        val nameWithSchema = getNameWithSchema(datamartMnemonic, entityName);
        return executor.delete(getTargetPath(datamartMnemonic, entityName), -1)
                .<Void>mapEmpty()
                .otherwise(error -> {
                    if (error instanceof KeeperException.NoNodeException) {
                        throw warn(new EntityNotExistsException(nameWithSchema));
                    } else {
                        throw new DtmException(String.format("Can't delete entity [%s]", nameWithSchema), error);
                    }
                });
    }

    @Cacheable(
            value = CacheConfiguration.ENTITY_CACHE,
            key = "new ru.datamart.prostore.query.execution.core.base.dto.cache.EntityKey(#datamartMnemonic, #entityName)"
    )
    public Future<Entity> getEntity(String datamartMnemonic, String entityName) {
        val nameWithSchema = getNameWithSchema(datamartMnemonic, entityName);
        return executor.getData(getTargetPath(datamartMnemonic, entityName))
                .map(entityData -> deserialize(entityData, Entity.class))
                .otherwise(error -> {
                    if (error instanceof KeeperException.NoNodeException) {
                        throw warn(new EntityNotExistsException((nameWithSchema)));
                    } else {
                        throw new DtmException(String.format("Can't get entity [%s]", nameWithSchema), error);
                    }
                });
    }

    public Future<List<String>> getEntityNamesByDatamart(String datamartMnemonic) {
        return executor.getChildren(getEntitiesPath(datamartMnemonic))
                .otherwise(error -> {
                    if (error instanceof KeeperException.NoNodeException) {
                        throw warn(new DatamartNotExistsException(datamartMnemonic));
                    } else {
                        throw new DtmException(String.format("Can't get entity names by datamart [%s]",
                                datamartMnemonic), error);
                    }
                });
    }

    @Override
    public String getTargetPath(Entity target) {
        return getTargetPath(target.getSchema(), target.getName());
    }

    private RuntimeException warn(RuntimeException error) {
        log.warn(error.getMessage(), error);
        return error;
    }

    private String getTargetPath(String datamartMnemonic, String entityName) {
        return String.format("%s/%s/entity/%s", envPath, datamartMnemonic, entityName);
    }

    private String getChangelogPath(String datamart) {
        return String.format("%s/%s/changelog", envPath, datamart);
    }

    private String getBlockPath(String datamart) {
        return String.format("%s/%s/block", envPath, datamart);
    }

    private String getImmutablePath(String datamart) {
        return String.format("%s/%s/immutable", envPath, datamart);
    }

    private String getEntitiesPath(String datamartMnemonic) {
        return String.format("%s/%s/entity", envPath, datamartMnemonic);
    }

    private String getNameWithSchema(String datamartMnemonic, String entityName) {
        return datamartMnemonic + "." + entityName;
    }

    private String toSequenceNumber(long number) {
        return SEQUENCE_NUMBER_TEMPLATE.substring(String.valueOf(number).length()) + number;
    }
}
