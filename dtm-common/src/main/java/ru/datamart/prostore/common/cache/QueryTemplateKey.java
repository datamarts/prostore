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
package ru.datamart.prostore.common.cache;

import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Objects;

@Data
@Builder
public class QueryTemplateKey {
    private String sourceQueryTemplate;
    private List<Datamart> logicalSchema;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryTemplateKey that = (QueryTemplateKey) o;
        return sourceQueryTemplate.equals(that.sourceQueryTemplate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceQueryTemplate);
    }
}
