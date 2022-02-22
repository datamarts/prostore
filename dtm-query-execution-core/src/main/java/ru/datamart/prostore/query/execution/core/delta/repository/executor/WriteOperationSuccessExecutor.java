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

import ru.datamart.prostore.common.exception.CrashException;
import ru.datamart.prostore.query.execution.core.aspect.RetryableFuture;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import ru.datamart.prostore.query.execution.core.delta.dto.Delta;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaWriteOp;
import ru.datamart.prostore.query.execution.core.delta.dto.HotDelta;
import ru.datamart.prostore.query.execution.core.delta.dto.operation.WriteOpFinish;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaException;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaNotExistException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
public class WriteOperationSuccessExecutor extends DeltaServiceDaoExecutorHelper implements DeltaDaoExecutor {

    @Autowired
    public WriteOperationSuccessExecutor(ZookeeperExecutor executor,
                                         @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    @RetryableFuture(retries = 5, cause = KeeperException.BadVersionException.class)
    public Future<Void> execute(String datamart, long sysCn) {
        Promise<Void> resultPromise = Promise.promise();
        val deltaStat = new Stat();
        val writeOpStat = new Stat();
        val ctx = new WriteOpContext(datamart, sysCn);
        getDatamartDeltaData(datamart, deltaStat)
                .map(bytes -> {
                    val delta = deserializedDelta(bytes);
                    if (delta.getHot() == null) {
                        throw new CrashException("Delta hot not exists", new DeltaNotExistException());
                    }
                    ctx.setDelta(delta);
                    ctx.setOpNum(sysCn - delta.getHot().getCnFrom());
                    ctx.setDeltaVersion(deltaStat.getVersion());
                    return ctx;
                })
                .compose(writeOpCtx -> getWriteOpData(datamart, writeOpStat, writeOpCtx))
                .map(this::deserializeDeltaWriteOp)
                .map(writeOp -> {
                    ctx.setWriteOpVersion(writeOpStat.getVersion());
                    ctx.setWriteOp(writeOp);
                    return writeOp;
                })
                .compose(writeOp -> executor.getChildren(getDatamartPath(datamart) + "/run"))
                .map(opPaths -> updateDeltaHot(ctx, opPaths))
                .compose(delta -> executor.multi(getUpdateOperationNodesOps(ctx)))
                .onSuccess(delta -> {
                    log.debug("Write delta operation \"success\" by datamart[{}], sysCn[{}] completed successfully", datamart, sysCn);
                    resultPromise.complete();
                })
                .onFailure(error -> {
                    val errMsg = String.format("Can't write operation \"success\" by datamart[%s], sysCn[%d]",
                            datamart,
                            sysCn);
                    if (error instanceof DeltaException) {
                        resultPromise.fail(error);
                    } else {
                        resultPromise.fail(new DeltaException(errMsg, error));
                    }
                });
        return resultPromise.future();
    }

    private Future<byte[]> getWriteOpData(String datamart, Stat writeOpStat, WriteOpContext ctx) {
        return getZkNodeData(writeOpStat, getWriteOpPath(datamart, ctx.getOpNum()));
    }

    private Future<byte[]> getDatamartDeltaData(String datamart, Stat deltaStat) {
        return getZkNodeData(deltaStat, getDeltaPath(datamart));
    }

    private Future<byte[]> getZkNodeData(Stat stat, String nodePath) {
        return executor.getData(nodePath, null, stat);
    }

    private Iterable<Op> getUpdateOperationNodesOps(WriteOpContext ctx) {
        return Arrays.asList(
                Op.delete(getWriteOpPath(ctx.getDatamart(), ctx.getOpNum()), ctx.getWriteOpVersion()),
                Op.delete(getBlockTablePath(ctx), -1),
                Op.setData(getDeltaPath(ctx.getDatamart()), serializedDelta(ctx.getDelta()), ctx.getDeltaVersion())
        );
    }

    private String getBlockTablePath(WriteOpContext ctx) {
        return getDatamartPath(ctx.getDatamart()) + "/block/" + ctx.getWriteOp().getTableName();
    }

    private HotDelta updateDeltaHot(WriteOpContext ctx, List<String> opPaths) {
        List<Long> opNums = opPaths.stream()
                .map(path -> Long.parseLong(path.substring(path.lastIndexOf("/") + 1)))
                .collect(Collectors.toList());
        val hotDelta = ctx.getDelta().getHot();
        val opN = ctx.getSysCn() - hotDelta.getCnFrom();
        val opMax = hotDelta.getCnMax() - hotDelta.getCnFrom();
        val cnMax = Math.max(ctx.getSysCn(), hotDelta.getCnMax());
        initWriteOperationsFinished(ctx, hotDelta);
        getLastOpDoneInSequence(opN, opMax, opNums).ifPresent(lastOpDone -> {
            val cnTo = hotDelta.getCnFrom() + lastOpDone;
            hotDelta.setCnTo(cnTo);
        });
        hotDelta.setCnMax(cnMax);
        return hotDelta;
    }

    private void initWriteOperationsFinished(WriteOpContext ctx, HotDelta hotDelta) {
        if (hotDelta.getWriteOperationsFinished() == null) {
            List<WriteOpFinish> writeOpFinishList = new ArrayList<>();
            writeOpFinishList.add(createNewWriteOpFinish(ctx));
            hotDelta.setWriteOperationsFinished(writeOpFinishList);
        } else {
            val writeOpFinish = hotDelta.getWriteOperationsFinished().stream()
                    .filter(wof -> wof.getTableName().equals(ctx.getWriteOp().getTableName()))
                    .findFirst();
            if (writeOpFinish.isPresent()) {
                writeOpFinish.get().getCnList().add(ctx.getSysCn());
            } else {
                hotDelta.getWriteOperationsFinished().add(createNewWriteOpFinish(ctx));
            }
        }
    }

    private WriteOpFinish createNewWriteOpFinish(WriteOpContext ctx) {
        List<Long> cnList = new ArrayList<>();
        cnList.add(ctx.getSysCn());
        return new WriteOpFinish(ctx.getWriteOp().getTableName(), cnList);
    }

    private Optional<Long> getLastOpDoneInSequence(long opN, long opMax, List<Long> opNums) {
        if (opNums.stream().anyMatch(op -> op < opN)) {
            return Optional.empty();
        } else {
            return Optional.of(
                    opNums.stream()
                            .filter(op -> op > opN)
                            .min(Comparator.naturalOrder())
                            .map(minOp -> minOp - 1)
                            .orElse(Math.max(opN, opMax))
            );
        }
    }

    @Override
    public Class<? extends DeltaDaoExecutor> getExecutorInterface() {
        return WriteOperationSuccessExecutor.class;
    }

    @Data
    private static final class WriteOpContext {
        private final String datamart;
        private final long sysCn;
        private DeltaWriteOp writeOp;
        private int writeOpVersion;
        private int deltaVersion;
        private Delta delta;
        private long opNum;
    }
}
