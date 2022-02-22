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
package ru.datamart.prostore.query.execution.plugin.adqm.check.service;

import ru.datamart.prostore.query.execution.plugin.adqm.base.dto.metadata.AdqmTableColumn;
import ru.datamart.prostore.query.execution.plugin.adqm.base.dto.metadata.AdqmTableEntity;
import ru.datamart.prostore.query.execution.plugin.adqm.base.dto.metadata.AdqmTables;
import ru.datamart.prostore.query.execution.plugin.adqm.base.factory.AdqmTableEntitiesFactory;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckException;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckTableRequest;
import ru.datamart.prostore.query.execution.plugin.api.factory.MetaTableEntityFactory;
import ru.datamart.prostore.query.execution.plugin.api.service.check.CheckTableService;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ru.datamart.prostore.query.execution.plugin.adqm.base.factory.AdqmMetaTableEntityFactory.DATA_TYPE;

@Service("adqmCheckTableService")
public class AdqmCheckTableService implements CheckTableService {

    public static final String SORTED_KEY_ERROR_TEMPLATE = "\tSorted keys are not equal expected [%s], got [%s].";
    private final AdqmTableEntitiesFactory adqmTableEntitiesFactory;
    private final MetaTableEntityFactory<AdqmTableEntity> metaTableEntityFactory;

    @Autowired
    public AdqmCheckTableService(AdqmTableEntitiesFactory adqmTableEntitiesFactory,
                                 MetaTableEntityFactory<AdqmTableEntity> metaTableEntityFactory) {
        this.adqmTableEntitiesFactory = adqmTableEntitiesFactory;
        this.metaTableEntityFactory = metaTableEntityFactory;
    }

    @Override
    public Future<Void> check(CheckTableRequest request) {
        AdqmTables<AdqmTableEntity> tableEntities = adqmTableEntitiesFactory
                .create(request.getEntity(), request.getEnvName());
        return Future.future(promise -> CompositeFuture.join(Stream.of(
                tableEntities.getShard(), tableEntities.getDistributed())
                .map(this::compare)
                .collect(Collectors.toList()))
                .onSuccess(result -> {
                    List<Optional<String>> list = result.list();
                    String errors = list.stream()
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .collect(Collectors.joining("\n"));
                    if (errors.isEmpty()) {
                        promise.complete();
                    } else {
                        promise.fail(new CheckException("\n" + errors));
                    }
                })
                .onFailure(promise::fail));
    }

    private Future<Optional<String>> compare(AdqmTableEntity expTableEntity) {
        return metaTableEntityFactory.create(expTableEntity.getEnv(), expTableEntity.getSchema(),
                expTableEntity.getName())
                .compose(optTableEntity -> Future.succeededFuture(optTableEntity
                        .map(tableEntity -> compare(tableEntity, expTableEntity))
                        .orElse(Optional.of(String.format(TABLE_NOT_EXIST_ERROR_TEMPLATE, expTableEntity.getName())))));
    }

    private Optional<String> compare(AdqmTableEntity tableEntity,
                                     AdqmTableEntity expTableEntity) {

        List<String> errors = new ArrayList<>();

        Set<String> expSortedKeys = Optional.ofNullable(expTableEntity.getSortedKeys())
                .map(strings -> (Set<String>) new HashSet<>(strings))
                .orElse(Collections.emptySet());
        List<String> sortedKeys = Optional.ofNullable(tableEntity.getSortedKeys()).orElse(Collections.emptyList());
        if (!(expSortedKeys.size() == sortedKeys.size() && expSortedKeys.containsAll(sortedKeys))) {
            errors.add(String.format(SORTED_KEY_ERROR_TEMPLATE,
                    String.join(", ", expSortedKeys),
                    String.join(", ", sortedKeys)));
        }
        Map<String, AdqmTableColumn> realColumns = tableEntity.getColumns().stream()
                .collect(Collectors.toMap(AdqmTableColumn::getName, Function.identity()));
        expTableEntity.getColumns().forEach(column -> {
            AdqmTableColumn realColumn = realColumns.get(column.getName());
            if (realColumn == null) {
                errors.add(String.format(COLUMN_NOT_EXIST_ERROR_TEMPLATE, column.getName()));
            } else {
                String realType = realColumn.getType();
                String type = column.getType();
                if (!Objects.equals(type, realType)) {
                    errors.add(String.format("\tColumn `%s`:", column.getName()));
                    errors.add(String.format(FIELD_ERROR_TEMPLATE, DATA_TYPE,
                            column.getType(), realColumn.getType()));
                }
            }
        });
        return errors.isEmpty()
                ? Optional.empty()
                : Optional.of(String.format("Table `%s.%s`:\n%s",
                expTableEntity.getSchema(),
                expTableEntity.getName(),
                String.join("\n", errors)));
    }
}
