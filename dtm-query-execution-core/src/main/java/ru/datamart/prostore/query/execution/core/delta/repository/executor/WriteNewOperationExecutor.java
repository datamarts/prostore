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
package ru.datamart.prostore.query.execution.core.delta.repository.executor;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.zookeeper.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaWriteOp;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaWriteOpRequest;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaClosedException;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaException;
import ru.datamart.prostore.query.execution.core.delta.exception.TableBlockedException;

import java.util.Arrays;
import java.util.List;

@Slf4j
@Component
public class WriteNewOperationExecutor extends DeltaServiceDaoExecutorHelper implements DeltaDaoExecutor {

    private static final int CREATE_OP_PATH_INDEX = 1;

    @Autowired
    public WriteNewOperationExecutor(ZookeeperExecutor executor,
                                     @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    public Future<Long> execute(DeltaWriteOpRequest request) {
        Promise<Long> resultPromise = Promise.promise();
        executor.getData(getDeltaPath(request.getDatamart()))
            .map(bytes -> {
                val delta = deserializedDelta(bytes);
                if (delta.getHot() == null) {
                    throw new DeltaClosedException();
                }
                return delta.getHot().getCnFrom();
            })
            .compose(cnFrom -> executor.multi(getWriteNewOps(request, cnFrom))
                .map(result -> getSysCn(cnFrom, result)))
            .onSuccess(sysCn -> {
                log.debug("Write new delta operation by datamart[{}] completed successfully: sysCn[{}]", request, sysCn);
                resultPromise.complete(sysCn);
            })
            .onFailure(error -> {
                val errMsg = String.format("Can't write new operation on datamart[%s]",
                    request.getDatamart());
                if (error instanceof KeeperException.NodeExistsException) {
                    resultPromise.fail(new TableBlockedException(request.getTableName(), error));
                } else if (error instanceof DeltaException) {
                    resultPromise.fail(error);
                } else {
                    resultPromise.fail(new DeltaException(errMsg, error));
                }
            });
        return resultPromise.future();
    }

    private Long getSysCn(Long cnFrom, List<OpResult> result) {
        if (result.size() == 2 && result.get(CREATE_OP_PATH_INDEX) instanceof OpResult.CreateResult) {
            try {
                val opPath = ((OpResult.CreateResult) result.get(CREATE_OP_PATH_INDEX)).getPath();
                val opNumber = Long.parseLong(opPath.substring(opPath.lastIndexOf("/") + 1));
                return opNumber + cnFrom;
            } catch (NumberFormatException e) {
                throw new DeltaException("Can't get op number", e);
            }
        } else {
            throw new DeltaException("Can't create sequential op node");
        }
    }

    private Iterable<Op> getWriteNewOps(DeltaWriteOpRequest request, long cnFrom) {
        return Arrays.asList(
            createDatamartNodeOp(getDatamartPath(request.getDatamart()), "/block/" + request.getTableName()),
            Op.create(getDatamartPath(request.getDatamart()) + "/run/",
                getWriteOpData(request, cnFrom),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL)
        );
    }

    private byte[] getWriteOpData(DeltaWriteOpRequest request, long cnFrom) {
        val deltaWriteOp = DeltaWriteOp.builder()
            .cnFrom(cnFrom)
            .tableNameExt(request.getTableNameExt())
            .tableName(request.getTableName())
            .query(request.getQuery())
            .status(0)
            .build();
        return serializeDeltaWriteOp(deltaWriteOp);
    }

    @Override
    public Class<? extends DeltaDaoExecutor> getExecutorInterface() {
        return WriteNewOperationExecutor.class;
    }
}
