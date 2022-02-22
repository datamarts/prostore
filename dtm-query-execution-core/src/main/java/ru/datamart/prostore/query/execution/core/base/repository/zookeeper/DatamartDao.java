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
import org.apache.zookeeper.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.query.execution.core.base.dto.metadata.DatamartInfo;
import ru.datamart.prostore.query.execution.core.base.exception.datamart.DatamartAlreadyExistsException;
import ru.datamart.prostore.query.execution.core.base.exception.datamart.DatamartNotExistsException;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import ru.datamart.prostore.query.execution.core.delta.dto.Delta;
import ru.datamart.prostore.serialization.CoreSerialization;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Repository
public class DatamartDao implements ZookeeperDao<String> {
    private static final int CREATE_DATAMART_OP_INDEX = 0;
    private static final byte[] EMPTY_DATA = null;
    private final ZookeeperExecutor executor;
    private final String envPath;

    @Autowired
    public DatamartDao(@Qualifier("zookeeperExecutor") ZookeeperExecutor executor,
                       @Value("${core.env.name}") String systemName) {
        this.executor = executor;
        this.envPath = "/" + systemName;
    }

    public Future<Void> createDatamart(String name) {
        return executor.createEmptyPersistentPath(envPath)
                .otherwise(error -> {
                    if (error instanceof KeeperException.NodeExistsException) {
                        return envPath;
                    } else {
                        throw new DtmException(
                                String.format("Can't create datamart [%s]", name),
                                error);
                    }
                })
                .compose(r -> executor.multi(getCreateDatamartOps(getTargetPath(name))))
                .otherwise(error -> {
                    if (error instanceof KeeperException.NodeExistsException) {
                        if (isDatamartExists((KeeperException) error)) {
                            throw new DatamartAlreadyExistsException(name);
                        }
                    }
                    throw new DtmException(String.format("Can't create datamart [%s]",
                            name),
                            error);
                })
                .<Void>mapEmpty()
                .onSuccess(s -> log.info("Datamart [{}] successfully created", name));
    }

    private List<Op> getCreateDatamartOps(String datamartPath) {
        byte[] deltaData = CoreSerialization.serialize(new Delta());
        return Arrays.asList(
                Op.create(datamartPath, EMPTY_DATA, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                createDatamartNodeOp(datamartPath, "/entity"),
                Op.create(datamartPath + "/delta", deltaData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                createDatamartNodeOp(datamartPath, "/delta/num"),
                createDatamartNodeOp(datamartPath, "/delta/date"),
                createDatamartNodeOp(datamartPath, "/changelog")
        );
    }

    private Op createDatamartNodeOp(String datamartPath, String nodeName) {
        return Op.create(datamartPath + nodeName, EMPTY_DATA, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    private boolean isDatamartExists(KeeperException error) {
        List<OpResult> results = error.getResults() == null ? Collections.emptyList() : error.getResults();
        return !results.isEmpty() && results.get(CREATE_DATAMART_OP_INDEX) instanceof OpResult.ErrorResult;
    }

    public Future<List<DatamartInfo>> getDatamartMeta() {
        return getDatamarts()
                .map(names -> names.stream()
                        .map(DatamartInfo::new)
                        .collect(Collectors.toList()
                        ));
    }

    public Future<List<String>> getDatamarts() {
        return executor.getChildren(envPath)
                .otherwise(error -> {
                    if (error instanceof KeeperException.NoNodeException) {
                        throw new DtmException(
                                String.format("Env [%s] not exists", envPath),
                                error);
                    } else {
                        throw new DtmException("Can't get datamarts", error);
                    }
                });
    }

    public Future<byte[]> getDatamart(String name) {
        return executor.getData(getTargetPath(name))
                .otherwise(error -> {
                    if (error instanceof KeeperException.NoNodeException) {
                        throw new DatamartNotExistsException(name);
                    } else {
                        throw new DtmException(
                                String.format("Can't get datamarts [%s]", name),
                                error);
                    }
                });
    }

    public Future<Boolean> existsDatamart(String name) {
        return executor.exists(getTargetPath(name));
    }

    public Future<Void> deleteDatamart(String name) {
        return executor.deleteRecursive(getTargetPath(name))
                .otherwise(error -> {
                    if (error instanceof IllegalArgumentException) {
                        throw new DatamartNotExistsException(name);
                    } else {
                        throw new DtmException(String.format("Can't delete datamarts [%s]",
                                name),
                                error);
                    }
                })
                .onSuccess(s -> log.info("Datamart [{}] successfully removed", name));
    }

    @Override
    public String getTargetPath(String target) {
        return String.format("%s/%s", envPath, target);
    }
}
