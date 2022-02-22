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
package ru.datamart.prostore.query.execution.plugin.adp.check.service;

import ru.datamart.prostore.query.execution.plugin.adp.base.dto.metadata.AdpTableColumn;
import ru.datamart.prostore.query.execution.plugin.adp.base.dto.metadata.AdpTableEntity;
import ru.datamart.prostore.query.execution.plugin.adp.base.dto.metadata.AdpTables;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckException;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckTableRequest;
import ru.datamart.prostore.query.execution.plugin.api.factory.MetaTableEntityFactory;
import ru.datamart.prostore.query.execution.plugin.api.factory.TableEntitiesFactory;
import ru.datamart.prostore.query.execution.plugin.api.service.check.CheckTableService;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ru.datamart.prostore.query.execution.plugin.api.factory.MetaTableEntityFactory.DATA_TYPE;

@Service("adpCheckTableService")
public class AdpCheckTableService implements CheckTableService {
    public static final String PRIMARY_KEY_ERROR_TEMPLATE = "\tPrimary keys are not equal expected [%s], got [%s].";

    private final TableEntitiesFactory<AdpTables<AdpTableEntity>> tableEntitiesFactory;
    private final MetaTableEntityFactory<AdpTableEntity> metaTableEntityFactory;

    @Autowired
    public AdpCheckTableService(TableEntitiesFactory<AdpTables<AdpTableEntity>> tableEntitiesFactory,
                                MetaTableEntityFactory<AdpTableEntity> metaTableEntityFactory) {
        this.tableEntitiesFactory = tableEntitiesFactory;
        this.metaTableEntityFactory = metaTableEntityFactory;
    }

    @Override
    public Future<Void> check(CheckTableRequest request) {
        AdpTables<AdpTableEntity> adpCreateTableQueries = tableEntitiesFactory
                .create(request.getEntity(), request.getEnvName());
        return Future.future(promise -> CompositeFuture.join(Stream.of(
                adpCreateTableQueries.getActual(),
                adpCreateTableQueries.getHistory(),
                adpCreateTableQueries.getStaging())
                .map(this::compare)
                .collect(Collectors.toList()))
                .onSuccess(result -> {
                    List<Optional<String>> list = result.list();
                    String errors = list.stream().filter(Optional::isPresent)
                            .map(Optional::get)
                            .collect(Collectors.joining("\n"));
                    if (errors.isEmpty()) {
                        promise.complete();
                    } else {
                        promise.fail(new CheckException("\n" + errors));
                    }
                })
                .onFailure(promise::fail)
        );
    }

    private Future<Optional<String>> compare(AdpTableEntity expectedTableEntity) {
        return metaTableEntityFactory.create(null, expectedTableEntity.getSchema(), expectedTableEntity.getName())
                .map(optTableEntity -> optTableEntity
                        .map(tableEntity -> compare(tableEntity, expectedTableEntity))
                        .orElse(Optional.of(String.format(TABLE_NOT_EXIST_ERROR_TEMPLATE, expectedTableEntity.getName()))));
    }

    private Optional<String> compare(AdpTableEntity actualTableEntity,
                                     AdpTableEntity expectedTableEntity) {
        List<String> errors = new ArrayList<>();
        if (!Objects.equals(expectedTableEntity.getPrimaryKeys(), actualTableEntity.getPrimaryKeys())) {
            errors.add(String.format(PRIMARY_KEY_ERROR_TEMPLATE,
                    String.join(", ", expectedTableEntity.getPrimaryKeys()),
                    String.join(", ", actualTableEntity.getPrimaryKeys())));
        }
        Map<String, AdpTableColumn> realColumns = actualTableEntity.getColumns().stream()
                .collect(Collectors.toMap(AdpTableColumn::getName, Function.identity()));
        expectedTableEntity.getColumns().forEach(column -> {
            AdpTableColumn realColumn = realColumns.get(column.getName());
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
                : Optional.of(String.format("Table `%s.%s`:%n%s", expectedTableEntity.getSchema(),
                expectedTableEntity.getName(), String.join("\n", errors)));
    }
}
