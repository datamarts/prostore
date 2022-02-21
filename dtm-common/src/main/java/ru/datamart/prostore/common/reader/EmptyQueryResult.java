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
package ru.datamart.prostore.common.reader;

import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Empty query result.
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class EmptyQueryResult extends QueryResult {
    public EmptyQueryResult() {
        super(null, Collections.emptyList());
    }

    @Override
    public void setResult(List<Map<String, Object>> result) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setMetadata(List<ColumnMetadata> metadata) {
        throw new UnsupportedOperationException();
    }
}
