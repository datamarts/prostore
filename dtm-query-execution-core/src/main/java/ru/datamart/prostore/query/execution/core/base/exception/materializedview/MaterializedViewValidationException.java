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
package ru.datamart.prostore.query.execution.core.base.exception.materializedview;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;

import java.util.List;
import java.util.Set;

public class MaterializedViewValidationException extends DtmException {
    private static final String MULTIPLE_DATAMARTS_PATTERN = "Materialized view %s has multiple datamarts: %s in query: %s";
    private static final String DIFFERENT_DATAMARTS_PATTERN = "Materialized view %s has datamart [%s] not equal to query [%s]";
    private static final String CONFLICT_COLUMN_COUNT_PATTERN = "Materialized view %s has conflict with query columns wrong count, view: %d query: %d";
    private static final String QUERY_DATASOURCE_TYPE = "Materialized view %s query DATASOURCE_TYPE not specified or invalid";
    private static final String VIEW_DATASOURCE_TYPE = "Materialized view %s DATASOURCE_TYPE has non exist items: %s";
    private static final String COLUMN_NOT_DEFINED_PATTERN = "Materialized view %s columns are not defined";

    private MaterializedViewValidationException(String message) {
        super(message);
    }

    public static MaterializedViewValidationException multipleDatamarts(String name, List<Datamart> datamarts, String query) {
        return new MaterializedViewValidationException(String.format(MULTIPLE_DATAMARTS_PATTERN, name, datamarts, query));
    }

    public static MaterializedViewValidationException differentDatamarts(String name, String entityDatamart, String queryDatamart) {
        return new MaterializedViewValidationException(String.format(DIFFERENT_DATAMARTS_PATTERN, name, entityDatamart, queryDatamart));
    }

    public static MaterializedViewValidationException columnCountConflict(String name, int viewColumns, int queryColumns) {
        return new MaterializedViewValidationException(String.format(CONFLICT_COLUMN_COUNT_PATTERN, name, viewColumns, queryColumns));
    }

    public static MaterializedViewValidationException columnsNotDefined(String name) {
        return new MaterializedViewValidationException(String.format(COLUMN_NOT_DEFINED_PATTERN, name));
    }

    public static MaterializedViewValidationException queryDataSourceInvalid(String name) {
        return new MaterializedViewValidationException(String.format(QUERY_DATASOURCE_TYPE, name));
    }

    public static MaterializedViewValidationException viewDataSourceInvalid(String name, Set<SourceType> nonExistSourceTypes) {
        return new MaterializedViewValidationException(String.format(VIEW_DATASOURCE_TYPE, name, nonExistSourceTypes));
    }
}
