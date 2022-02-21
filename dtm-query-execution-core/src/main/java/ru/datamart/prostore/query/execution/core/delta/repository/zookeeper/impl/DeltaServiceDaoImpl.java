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
package ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.impl;

import ru.datamart.prostore.query.execution.core.base.configuration.CacheConfiguration;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaWriteOp;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaWriteOpRequest;
import ru.datamart.prostore.query.execution.core.delta.dto.HotDelta;
import ru.datamart.prostore.query.execution.core.delta.dto.OkDelta;
import ru.datamart.prostore.query.execution.core.delta.repository.executor.*;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaDaoExecutorRepository;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
public class DeltaServiceDaoImpl implements DeltaServiceDao, DeltaDaoExecutorRepository {
    private final Map<Class<? extends DeltaDaoExecutor>, DeltaDaoExecutor> executorMap;

    @Autowired
    public DeltaServiceDaoImpl(List<DeltaDaoExecutor> deltaDaoExecutors) {
        this.executorMap = deltaDaoExecutors.stream().collect(Collectors.toMap(DeltaDaoExecutor::getExecutorInterface, deltaDaoExecutor -> deltaDaoExecutor));
    }

    @Override
    @CacheEvict(cacheNames = {CacheConfiguration.OK_DELTA_CACHE, CacheConfiguration.HOT_DELTA_CACHE},
            key = "#datamart")
    public Future<Long> writeNewDeltaHot(String datamart) {
        return writeNewDeltaHot(datamart, null);
    }

    @Override
    @CacheEvict(cacheNames = {CacheConfiguration.OK_DELTA_CACHE, CacheConfiguration.HOT_DELTA_CACHE},
            key = "#datamart")
    public Future<Long> writeNewDeltaHot(String datamart, Long deltaHotNum) {
        return getExecutor(WriteNewDeltaHotExecutor.class).execute(datamart, deltaHotNum);
    }

    @Override
    @CacheEvict(cacheNames = {CacheConfiguration.OK_DELTA_CACHE, CacheConfiguration.HOT_DELTA_CACHE},
            key = "#datamart")
    public Future<OkDelta> writeDeltaHotSuccess(String datamart) {
        return writeDeltaHotSuccess(datamart, null);
    }

    @Override
    @CacheEvict(cacheNames = {CacheConfiguration.OK_DELTA_CACHE, CacheConfiguration.HOT_DELTA_CACHE},
            key = "#datamart")
    public Future<OkDelta> writeDeltaHotSuccess(String datamart, LocalDateTime deltaHotDate) {
        return getExecutor(WriteDeltaHotSuccessExecutor.class).execute(datamart, deltaHotDate);
    }

    @Override
    @CacheEvict(cacheNames = {CacheConfiguration.OK_DELTA_CACHE, CacheConfiguration.HOT_DELTA_CACHE},
            key = "#datamart")
    public Future<Void> writeDeltaError(String datamart, Long deltaHotNum) {
        return getExecutor(WriteDeltaErrorExecutor.class).execute(datamart, deltaHotNum);
    }

    @Override
    @CacheEvict(cacheNames = {CacheConfiguration.OK_DELTA_CACHE, CacheConfiguration.HOT_DELTA_CACHE},
            key = "#datamart")
    public Future<Void> deleteDeltaHot(String datamart) {
        return getExecutor(DeleteDeltaHotExecutor.class).execute(datamart);
    }

    @Override
    public Future<Long> writeNewOperation(DeltaWriteOpRequest operation) {
        return getExecutor(WriteNewOperationExecutor.class).execute(operation);
    }

    @Override
    @CacheEvict(cacheNames = {CacheConfiguration.OK_DELTA_CACHE, CacheConfiguration.HOT_DELTA_CACHE},
            key = "#datamart")
    public Future<Void> writeOperationSuccess(String datamart, long synCn) {
        return getExecutor(WriteOperationSuccessExecutor.class).execute(datamart, synCn);
    }

    @Override
    public Future<Void> writeOperationError(String datamart, long synCn) {
        return getExecutor(WriteOperationErrorExecutor.class).execute(datamart, synCn);
    }

    @Override
    public Future<Void> deleteWriteOperation(String datamart, long synCn) {
        return getExecutor(DeleteWriteOperationExecutor.class).execute(datamart, synCn);
    }

    @Override
    public Future<OkDelta> getDeltaByNum(String datamart, long num) {
        return getExecutor(GetDeltaByNumExecutor.class).execute(datamart, num);
    }

    @Override
    public Future<OkDelta> getDeltaByDateTime(String datamart, LocalDateTime dateTime) {
        return getExecutor(GetDeltaByDateTimeExecutor.class).execute(datamart, dateTime);
    }

    @Override
    @Cacheable(value = CacheConfiguration.OK_DELTA_CACHE, key = "#datamart")
    public Future<OkDelta> getDeltaOk(String datamart) {
        return getExecutor(GetDeltaOkExecutor.class).execute(datamart);
    }

    @Override
    @Cacheable(value = CacheConfiguration.HOT_DELTA_CACHE, key = "#datamart")
    public Future<HotDelta> getDeltaHot(String datamart) {
        return getExecutor(GetDeltaHotExecutor.class).execute(datamart);
    }

    @Override
    public Future<List<DeltaWriteOp>> getDeltaWriteOperations(String datamart) {
        return getExecutor(GetDeltaWriteOperationsExecutor.class).execute(datamart);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends DeltaDaoExecutor> T getExecutor(Class<T> executorInterface) {
        return (T) executorMap.get(executorInterface);
    }
}
