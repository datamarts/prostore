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
package io.arenadata.dtm.query.execution.core.base.repository.zookeeper;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.DenyChanges;
import io.arenadata.dtm.query.execution.core.base.repository.DaoUtils;
import io.arenadata.dtm.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import io.vertx.core.Future;
import lombok.val;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.util.Objects;

@Repository
public class ChangesDao {
    private static final String CHANGES_BLOCKED = "Changes are blocked by another process within %s";
    private static final String CHANGES_NOT_BLOCKED = "Changes are not blocked now within %s";
    private static final String UNEXPECTED_EXCEPTION = "Unexpected exception during %s";
    private static final String WRONG_CODE = "Wrong code";
    private static final String IMMUTABLE_PATH = "%s/%s/immutable";

    private final ZookeeperExecutor executor;
    private final String envPath;

    public ChangesDao(@Qualifier("zookeeperExecutor") ZookeeperExecutor executor,
                      @Value("${core.env.name}") String systemName) {
        this.executor = executor;
        this.envPath = "/" + systemName;
    }

    public Future<Void> denyChanges(String datamart, String denyCode) {
        val denyChanges = new DenyChanges(DaoUtils.getCurrentDateTime(), denyCode);
        return executor.create(getImmutablePath(datamart), DaoUtils.serialize(denyChanges), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
                .otherwise(e -> {
                    if (e instanceof KeeperException.NodeExistsException) {
                        throw new DtmException(String.format(CHANGES_BLOCKED, datamart), e);
                    }

                    throw new DtmException(String.format(UNEXPECTED_EXCEPTION, "denyChanges"), e);
                })
                .mapEmpty();
    }

    public Future<Void> allowChanges(String datamart, String denyCode) {
        return getImmutableNode(datamart)
                .map(bytes -> checkDenyCode(bytes, denyCode, datamart))
                .compose(ignore -> executor.delete(getImmutablePath(datamart), -1));
    }

    private String getImmutablePath(String datamart) {
        return String.format(IMMUTABLE_PATH, envPath, datamart);
    }

    private Future<byte[]> getImmutableNode(String datamart) {
        val immutablePath = getImmutablePath(datamart);
        return executor.getData(immutablePath)
                .otherwise(e -> {
                    if (e instanceof KeeperException.NoNodeException) {
                        throw new DtmException(String.format(CHANGES_NOT_BLOCKED, datamart), e);
                    }

                    throw new DtmException(String.format(UNEXPECTED_EXCEPTION, "allowChanges"), e);
                });
    }

    private DenyChanges checkDenyCode(byte[] bytes, String denyCode, String datamart) {
        if (bytes == null) {
            throw new DtmException(String.format("Unexpected null in %s node", getImmutablePath(datamart)));
        }

        val denyChanges = DaoUtils.deserialize(bytes, datamart, DenyChanges.class);
        if (denyChanges.getDenyCode() == null) {
            throw new DtmException(String.format(CHANGES_BLOCKED, datamart));
        }
        if (!Objects.equals(denyCode, denyChanges.getDenyCode())) {
            throw new DtmException(WRONG_CODE);
        }

        return denyChanges;
    }
}
