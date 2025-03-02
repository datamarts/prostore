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
package ru.datamart.prostore.query.execution.core.eddl.service;

import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.execution.core.eddl.dto.EddlAction;
import ru.datamart.prostore.query.execution.core.eddl.dto.EddlQuery;
import io.vertx.core.Future;

/**
 * EDDL query executor
 */
public interface EddlExecutor {

  /**
   * <p>Execute EDDL query</p>
   *  @param query              query
   * @return
   */
  Future<QueryResult> execute(EddlQuery query);

  /**
   * Get query type
   *
   * @return query type
   */
  EddlAction getAction();
}
