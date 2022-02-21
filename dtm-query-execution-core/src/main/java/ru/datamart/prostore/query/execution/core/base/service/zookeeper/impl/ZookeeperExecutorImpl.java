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
package ru.datamart.prostore.query.execution.core.base.service.zookeeper.impl;

import ru.datamart.prostore.common.util.ThrowableConsumer;
import ru.datamart.prostore.common.util.ThrowableFunction;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperConnectionProvider;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.RequiredArgsConstructor;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;

@RequiredArgsConstructor
public class ZookeeperExecutorImpl implements ZookeeperExecutor {
    private final ZookeeperConnectionProvider connectionManager;
    private final Vertx vertx;

    @Override
    public Future<byte[]> getData(String path) {
        return getData(path, null, null);
    }

    @Override
    public Future<byte[]> getData(String path, boolean watch, Stat stat) {
        return execute(zk -> zk.getData(path, watch, stat));
    }

    @Override
    public Future<byte[]> getData(String path, Watcher watcher, Stat stat) {
        return execute(zk -> zk.getData(path, watcher, stat));
    }

    @Override
    public Future<List<String>> getChildren(String path) {
        return getChildren(path, null);
    }

    @Override
    public Future<List<String>> getChildren(String path, Watcher watcher) {
        return execute(zk -> zk.getChildren(path, watcher));
    }

    @Override
    public Future<List<String>> getChildren(String path, boolean watch) {
        return execute(zk -> zk.getChildren(path, watch));
    }

    @Override
    public Future<String> createEmptyPersistentPath(String path) {
        return createPersistentPath(path, new byte[0]);
    }

    @Override
    public Future<String> createPersistentPath(String path, byte[] data) {
        return create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Override
    public Future<String> createPersistentSequentialPath(String path, byte[] data) {
        return create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
    }

    @Override
    public Future<String> create(final String path,
                                 byte[] data,
                                 List<ACL> acl,
                                 CreateMode createMode) {
        return execute(zk -> zk.create(path, data, acl, createMode));
    }

    @Override
    public Future<Stat> setData(String path, byte[] data, int version) {
        return execute(zk -> zk.setData(path, data, version));
    }

    @Override
    public Future<List<OpResult>> multi(Iterable<Op> ops) {
        return execute(zk -> zk.multi(ops));
    }

    @Override
    public Future<Void> delete(String path, int version) {
        return executeVoid(zk -> zk.delete(path, version));
    }

    @Override
    public Future<Void> deleteRecursive(String pathRoot) {
        return executeVoid(zk -> ZKUtil.deleteRecursive(zk, pathRoot));
    }

    @Override
    public <T> Future<T> execute(ThrowableFunction<ZooKeeper, T> function) {
        return Future.future(promise -> vertx.executeBlocking(blockingPromise -> {
            try {
                blockingPromise.complete(function.apply(connectionManager.getOrConnect()));
            } catch (Exception e) {
                blockingPromise.fail(e);
            }
        }, false, promise));
    }

    @Override
    public Future<Boolean> exists(String path) {
        return execute(zk -> zk.exists(path, false) != null);
    }

    @Override
    public Future<Void> executeVoid(ThrowableConsumer<ZooKeeper> consumer) {
        return Future.future(promise -> vertx.executeBlocking(blockingPromise -> {
            try {
                consumer.accept(connectionManager.getOrConnect());
                blockingPromise.complete();
            } catch (Exception e) {
                blockingPromise.fail(e);
            }
        }, promise));
    }
}
