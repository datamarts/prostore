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
package ru.datamart.prostore.query.execution.core.dml.service;

import io.vertx.core.Future;
import lombok.val;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.core.query.exception.NoSingleDataSourceContainsAllEntitiesException;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class AcceptableSourceTypesDefinitionService {

    public AcceptableSourceTypesDefinitionService() {
    }

    public Future<Set<SourceType>> define(List<Datamart> schema) {
        return Future.future(promise -> {
            val entities = getEntities(schema);
            promise.complete(getSourceTypes(entities));
        });
    }

    private List<Entity> getEntities(List<Datamart> schema) {
        if (schema == null) {
            return Collections.emptyList();
        }

        return schema.stream()
                .flatMap(datamart -> datamart.getEntities() != null ? datamart.getEntities().stream() : Stream.empty())
                .collect(Collectors.toList());
    }

    private Set<SourceType> getSourceTypes(List<Entity> entities) {
        val stResult = getCommonSourceTypes(entities);
        if (stResult.isEmpty()) {
            throw new NoSingleDataSourceContainsAllEntitiesException();
        }

        return stResult;
    }

    private Set<SourceType> getCommonSourceTypes(List<Entity> entities) {
        if (entities.isEmpty()) {
            return new HashSet<>();
        }

        val result = EnumSet.allOf(SourceType.class);
        entities.stream()
                .map(Entity::getDestination)
                .map(sourceTypes -> sourceTypes == null ? Collections.<SourceType>emptySet() : sourceTypes)
                .forEach(result::retainAll);
        return result;
    }
}
