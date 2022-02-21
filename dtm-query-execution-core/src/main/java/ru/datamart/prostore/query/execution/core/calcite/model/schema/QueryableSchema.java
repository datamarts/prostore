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
package ru.datamart.prostore.query.execution.core.calcite.model.schema;

import ru.datamart.prostore.query.execution.core.calcite.model.schema.dialect.CoreDtmConvention;
import org.apache.calcite.schema.impl.AbstractSchema;

public class QueryableSchema extends AbstractSchema {

  private CoreDtmConvention convention;

  public QueryableSchema(CoreDtmConvention convention) {
    this.convention = convention;
  }

  public CoreDtmConvention getConvention() {
    return convention;
  }
}
