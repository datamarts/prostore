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
package ru.datamart.prostore.query.calcite.core.framework;

import com.google.common.collect.ImmutableList;
import lombok.val;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.materialize.SqlStatisticProvider;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.statistic.QuerySqlStatisticProvider;
import org.apache.calcite.tools.*;

import java.util.List;
import java.util.Objects;

/**
 * This custom class is needed to be able to set its own implementation of Planner
 */
public class DtmCalciteFramework {

    public static Planner getPlanner(FrameworkConfig config) {
        return new DtmCalcitePlannerImpl(config);
    }

    public static SchemaPlus createRootSchema(boolean addMetadataSchema) {
        return CalciteSchema.createRootSchema(addMetadataSchema).plus();
    }

    public static DtmCalciteFramework.ConfigBuilder newConfigBuilder() {
        return new DtmCalciteFramework.ConfigBuilder();
    }

    public static DtmCalciteFramework.ConfigBuilder newConfigBuilder(FrameworkConfig config) {
        return new DtmCalciteFramework.ConfigBuilder(config);
    }

    @FunctionalInterface
    public interface BasePrepareAction<R> {
        R apply(RelOptCluster var1, RelOptSchema var2, SchemaPlus var3, CalciteServerStatement var4);
    }

    @FunctionalInterface
    public interface PlannerAction<R> {
        R apply(RelOptCluster var1, RelOptSchema var2, SchemaPlus var3);
    }

    static class StdFrameworkConfig implements FrameworkConfig {
        private final Context context;
        private final SqlRexConvertletTable convertletTable;
        private final SqlOperatorTable operatorTable;
        private final ImmutableList<Program> programs;
        private final ImmutableList<? extends RelTraitDef> traitDefs;
        private final SqlParser.Config parserConfig;
        private final org.apache.calcite.sql2rel.SqlToRelConverter.Config sqlToRelConverterConfig;
        private final SchemaPlus defaultSchema;
        private final RelOptCostFactory costFactory;
        private final RelDataTypeSystem typeSystem;
        private final RexExecutor executor;
        private final boolean evolveLattice;
        private final SqlStatisticProvider statisticProvider;
        private final RelOptTable.ViewExpander viewExpander;
        private final SqlValidator.Config validatorConfig;

        StdFrameworkConfig(Context context,
                           SqlRexConvertletTable convertletTable,
                           SqlOperatorTable operatorTable,
                           ImmutableList<Program> programs,
                           ImmutableList<? extends RelTraitDef> traitDefs,
                           SqlParser.Config parserConfig,
                           org.apache.calcite.sql2rel.SqlToRelConverter.Config sqlToRelConverterConfig,
                           SchemaPlus defaultSchema,
                           RelOptCostFactory costFactory,
                           RelDataTypeSystem typeSystem,
                           RexExecutor executor,
                           boolean evolveLattice,
                           SqlStatisticProvider statisticProvider,
                           RelOptTable.ViewExpander viewExpander,
                           SqlValidator.Config validatorConfig) {
            this.context = context;
            this.convertletTable = convertletTable;
            this.operatorTable = operatorTable;
            this.programs = programs;
            this.traitDefs = traitDefs;
            this.parserConfig = parserConfig;
            this.sqlToRelConverterConfig = sqlToRelConverterConfig;
            this.defaultSchema = defaultSchema;
            this.costFactory = costFactory;
            this.typeSystem = typeSystem;
            this.executor = executor;
            this.evolveLattice = evolveLattice;
            this.statisticProvider = statisticProvider;
            this.viewExpander = viewExpander;
            val config = validatorConfig == null ? SqlValidator.Config.DEFAULT : validatorConfig;
            this.validatorConfig = config.withCallRewrite(true);
        }

        public SqlParser.Config getParserConfig() {
            return this.parserConfig;
        }

        @Override
        public SqlValidator.Config getSqlValidatorConfig() {
            return this.validatorConfig;
        }

        public org.apache.calcite.sql2rel.SqlToRelConverter.Config getSqlToRelConverterConfig() {
            return this.sqlToRelConverterConfig;
        }

        public SchemaPlus getDefaultSchema() {
            return this.defaultSchema;
        }

        public RexExecutor getExecutor() {
            return this.executor;
        }

        public ImmutableList<Program> getPrograms() {
            return this.programs;
        }

        public RelOptCostFactory getCostFactory() {
            return this.costFactory;
        }

        public ImmutableList<RelTraitDef> getTraitDefs() {
            return (ImmutableList<RelTraitDef>) this.traitDefs;
        }

        public SqlRexConvertletTable getConvertletTable() {
            return this.convertletTable;
        }

        public Context getContext() {
            return this.context;
        }

        public SqlOperatorTable getOperatorTable() {
            return this.operatorTable;
        }

        public RelDataTypeSystem getTypeSystem() {
            return this.typeSystem;
        }

        public boolean isEvolveLattice() {
            return this.evolveLattice;
        }

        public SqlStatisticProvider getStatisticProvider() {
            return this.statisticProvider;
        }

        public RelOptTable.ViewExpander getViewExpander() {
            return this.viewExpander;
        }
    }

    public static class ConfigBuilder {
        private SqlRexConvertletTable convertletTable;
        private SqlOperatorTable operatorTable;
        private ImmutableList<Program> programs;
        private Context context;
        private ImmutableList<? extends RelTraitDef> traitDefs;
        private SqlParser.Config parserConfig;
        private org.apache.calcite.sql2rel.SqlToRelConverter.Config sqlToRelConverterConfig;
        private SchemaPlus defaultSchema;
        private RexExecutor executor;
        private RelOptCostFactory costFactory;
        private RelDataTypeSystem typeSystem;
        private boolean evolveLattice;
        private SqlStatisticProvider statisticProvider;
        private RelOptTable.ViewExpander viewExpander;
        private SqlValidator.Config validatorConfig;

        private ConfigBuilder() {
            this.convertletTable = StandardConvertletTable.INSTANCE;
            this.operatorTable = SqlExtOperatorTable.instance();
            this.programs = ImmutableList.of();
            this.context = Contexts.empty();
            this.parserConfig = SqlParser.Config.DEFAULT;
            this.sqlToRelConverterConfig = org.apache.calcite.sql2rel.SqlToRelConverter.Config.DEFAULT;
            this.typeSystem = RelDataTypeSystem.DEFAULT;
            this.evolveLattice = false;
            this.statisticProvider = QuerySqlStatisticProvider.SILENT_CACHING_INSTANCE;
        }

        private ConfigBuilder(FrameworkConfig config) {
            this.convertletTable = config.getConvertletTable();
            this.operatorTable = config.getOperatorTable();
            this.programs = config.getPrograms();
            this.context = config.getContext();
            this.traitDefs = config.getTraitDefs();
            this.parserConfig = config.getParserConfig();
            this.sqlToRelConverterConfig = config.getSqlToRelConverterConfig();
            this.defaultSchema = config.getDefaultSchema();
            this.executor = config.getExecutor();
            this.costFactory = config.getCostFactory();
            this.typeSystem = config.getTypeSystem();
            this.evolveLattice = config.isEvolveLattice();
            this.statisticProvider = config.getStatisticProvider();
            this.validatorConfig = config.getSqlValidatorConfig();
        }

        public FrameworkConfig build() {
            return new DtmCalciteFramework.StdFrameworkConfig(this.context,
                    this.convertletTable,
                    this.operatorTable,
                    this.programs,
                    this.traitDefs,
                    this.parserConfig,
                    this.sqlToRelConverterConfig,
                    this.defaultSchema,
                    this.costFactory,
                    this.typeSystem,
                    this.executor,
                    this.evolveLattice,
                    this.statisticProvider,
                    this.viewExpander,
                    this.validatorConfig);
        }

        public DtmCalciteFramework.ConfigBuilder context(Context c) {
            this.context = Objects.requireNonNull(c);
            return this;
        }

        public DtmCalciteFramework.ConfigBuilder executor(RexExecutor executor) {
            this.executor = Objects.requireNonNull(executor);
            return this;
        }

        public DtmCalciteFramework.ConfigBuilder convertletTable(SqlRexConvertletTable convertletTable) {
            this.convertletTable = Objects.requireNonNull(convertletTable);
            return this;
        }

        public DtmCalciteFramework.ConfigBuilder operatorTable(SqlOperatorTable operatorTable) {
            this.operatorTable = Objects.requireNonNull(operatorTable);
            return this;
        }

        public DtmCalciteFramework.ConfigBuilder traitDefs(List<? extends RelTraitDef> traitDefs) {
            if (traitDefs == null) {
                this.traitDefs = null;
            } else {
                this.traitDefs = ImmutableList.copyOf(traitDefs);
            }

            return this;
        }

        public DtmCalciteFramework.ConfigBuilder traitDefs(RelTraitDef... traitDefs) {
            this.traitDefs = ImmutableList.copyOf(traitDefs);
            return this;
        }

        public DtmCalciteFramework.ConfigBuilder parserConfig(SqlParser.Config parserConfig) {
            this.parserConfig = Objects.requireNonNull(parserConfig);
            return this;
        }

        public DtmCalciteFramework.ConfigBuilder sqlToRelConverterConfig(org.apache.calcite.sql2rel.SqlToRelConverter.Config sqlToRelConverterConfig) {
            this.sqlToRelConverterConfig = Objects.requireNonNull(sqlToRelConverterConfig);
            return this;
        }

        public DtmCalciteFramework.ConfigBuilder defaultSchema(SchemaPlus defaultSchema) {
            this.defaultSchema = defaultSchema;
            return this;
        }

        public DtmCalciteFramework.ConfigBuilder costFactory(RelOptCostFactory costFactory) {
            this.costFactory = costFactory;
            return this;
        }

        public DtmCalciteFramework.ConfigBuilder ruleSets(RuleSet... ruleSets) {
            return this.programs(Programs.listOf(ruleSets));
        }

        public DtmCalciteFramework.ConfigBuilder ruleSets(List<RuleSet> ruleSets) {
            return this.programs(Programs.listOf(Objects.requireNonNull(ruleSets)));
        }

        public DtmCalciteFramework.ConfigBuilder programs(List<Program> programs) {
            this.programs = ImmutableList.copyOf(programs);
            return this;
        }

        public DtmCalciteFramework.ConfigBuilder programs(Program... programs) {
            this.programs = ImmutableList.copyOf(programs);
            return this;
        }

        public DtmCalciteFramework.ConfigBuilder typeSystem(RelDataTypeSystem typeSystem) {
            this.typeSystem = Objects.requireNonNull(typeSystem);
            return this;
        }

        public DtmCalciteFramework.ConfigBuilder evolveLattice(boolean evolveLattice) {
            this.evolveLattice = evolveLattice;
            return this;
        }

        public DtmCalciteFramework.ConfigBuilder statisticProvider(SqlStatisticProvider statisticProvider) {
            this.statisticProvider = Objects.requireNonNull(statisticProvider);
            return this;
        }

        public DtmCalciteFramework.ConfigBuilder viewExpander(RelOptTable.ViewExpander viewExpander) {
            this.viewExpander = viewExpander;
            return this;
        }
    }
}
