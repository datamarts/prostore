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
package ru.datamart.prostore.query.calcite.core.provider;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.*;
import org.apache.calcite.util.Holder;
import ru.datamart.prostore.common.calcite.CalciteContext;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.query.calcite.core.factory.impl.CalciteSchemaFactory;
import ru.datamart.prostore.query.calcite.core.framework.DtmCalciteFramework;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
public abstract class CalciteContextProvider {
    protected final List<RelTraitDef> traitDefs;
    protected final RuleSet prepareRules;
    protected final SqlParser.Config configParser;
    protected final CalciteSchemaFactory calciteSchemaFactory;

    static {
        /*
            this hook disables simplify of REX nodes:
            CAST(TRUE as INTEGER) is not working with simplify as TRUE
            (this will produce NumberFormatException inside the calcite, as it trying to Integer.parseInt("true")
         */
        Hook.REL_BUILDER_SIMPLIFY.add(o -> {
            ((Holder<Object>) o).set(Boolean.FALSE);
        });
    }

    public CalciteContextProvider(SqlParser.Config configParser,
                                  CalciteSchemaFactory calciteSchemaFactory) {
        this.configParser = configParser;
        prepareRules =
                RuleSets.ofList(
                        EnumerableRules.ENUMERABLE_RULES);

        traitDefs = new ArrayList<>();
        traitDefs.add(ConventionTraitDef.INSTANCE);
        traitDefs.add(RelCollationTraitDef.INSTANCE);
        this.calciteSchemaFactory = calciteSchemaFactory;
    }

    public CalciteContext context(List<Datamart> schemas) {
        final SchemaPlus rootSchema = DtmCalciteFramework.createRootSchema(true);
        rootSchema.add(ColumnType.INT.name(), factory -> factory.createSqlType(SqlTypeName.BIGINT));
        rootSchema.add(ColumnType.INT32.name(), factory -> factory.createSqlType(SqlTypeName.INTEGER));
        rootSchema.add(ColumnType.LINK.name(), factory -> factory.createSqlType(SqlTypeName.VARCHAR));
        rootSchema.add(ColumnType.UUID.name(), factory -> factory.createSqlType(SqlTypeName.VARCHAR));

        Datamart defaultDatamart = null;
        if (schemas != null) {
            Optional<Datamart> defaultSchemaOptional = schemas.stream()
                    .filter(Datamart::getIsDefault)
                    .findFirst();
            if (defaultSchemaOptional.isPresent()) {
                defaultDatamart = defaultSchemaOptional.get();
            }
            schemas.stream()
                    .filter(d -> !d.getIsDefault())
                    .forEach(d -> calciteSchemaFactory.addSchema(rootSchema, d));
        }

        final SchemaPlus defaultSchema = defaultDatamart == null ?
                rootSchema : calciteSchemaFactory.addSchema(rootSchema, defaultDatamart);

        SqlToRelConverter.Config toRelConverterConfig = SqlToRelConverter.configBuilder()
                .withExpand(false)
                .withInSubQueryThreshold(65536)
                .build();
        FrameworkConfig config = DtmCalciteFramework.newConfigBuilder()
                .parserConfig(configParser)
                .defaultSchema(defaultSchema)
                .traitDefs(traitDefs).programs(Programs.of(prepareRules))
                .sqlToRelConverterConfig(toRelConverterConfig)
                .build();
        Planner planner = DtmCalciteFramework.getPlanner(config);
        return new CalciteContext(rootSchema, planner, RelBuilder.create(config));
    }

    public void enrichContext(CalciteContext context, List<Datamart> schemas) {
        schemas.forEach(s -> calciteSchemaFactory.addSchema(context.getSchema(), s));
    }
}
