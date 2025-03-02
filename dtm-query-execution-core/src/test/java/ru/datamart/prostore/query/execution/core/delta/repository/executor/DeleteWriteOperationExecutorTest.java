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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperExecutor;

import static org.junit.jupiter.api.Assertions.assertSame;

@ExtendWith(MockitoExtension.class)
class DeleteWriteOperationExecutorTest {
    private static final String ENV = "env";

    @Mock
    private ZookeeperExecutor zookeeperExecutor;

    private DeleteWriteOperationExecutor executor;

    @BeforeEach
    void setUp() {
        executor = new DeleteWriteOperationExecutor(zookeeperExecutor, ENV);
    }

    @Test
    void shouldBeCorrectClass() {
        assertSame(executor.getExecutorInterface(), DeleteWriteOperationExecutor.class);
    }
}