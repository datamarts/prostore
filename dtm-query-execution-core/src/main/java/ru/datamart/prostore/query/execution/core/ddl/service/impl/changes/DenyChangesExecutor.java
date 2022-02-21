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
package ru.datamart.prostore.query.execution.core.ddl.service.impl.changes;

import ru.datamart.prostore.query.calcite.core.extension.OperationNames;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.ChangesDao;
import ru.datamart.prostore.query.execution.core.ddl.service.ChangesExecutor;
import io.vertx.core.Future;
import org.springframework.stereotype.Component;

@Component
public class DenyChangesExecutor extends ChangesExecutor {

    public DenyChangesExecutor(ChangesDao changesDao) {
        super(changesDao);
    }

    @Override
    protected Future<Void> runOperation(String datamart, String denyCode) {
        return changesDao.denyChanges(datamart, denyCode);
    }

    @Override
    public String getOperationKind() {
        return OperationNames.DENY_CHANGES;
    }
}
