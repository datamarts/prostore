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
import com.google.common.collect.UnmodifiableIterator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.*;
import org.apache.calcite.util.Pair;
import ru.datamart.prostore.common.exception.DtmException;

import java.io.Reader;
import java.util.List;
import java.util.Properties;

/**
 * This custom class is needed to be able to set its own implementation of RelDataTypeSystem
 */
public class DtmCalcitePlannerImpl implements Planner, RelOptTable.ViewExpander {

    private final SqlOperatorTable operatorTable;
    private final ImmutableList<Program> programs;
    private final RelOptCostFactory costFactory;
    private final Context context;
    private final CalciteConnectionConfig connectionConfig;
    private final ImmutableList<RelTraitDef> traitDefs;
    private final SqlParser.Config parserConfig;
    private final org.apache.calcite.sql2rel.SqlToRelConverter.Config sqlToRelConverterConfig;
    private final SqlRexConvertletTable convertletTable;
    private final SqlValidator.Config config;
    private DtmCalcitePlannerImpl.State state;
    private SchemaPlus defaultSchema;
    private JavaTypeFactory typeFactory;
    private RelOptPlanner planner;
    private RexExecutor executor;
    private SqlValidator validator;
    private SqlNode validatedSqlNode;
    private RelRoot root;

    public DtmCalcitePlannerImpl(FrameworkConfig config) {
        this.costFactory = config.getCostFactory();
        this.defaultSchema = config.getDefaultSchema();
        this.operatorTable = config.getOperatorTable();
        this.programs = config.getPrograms();
        this.parserConfig = config.getParserConfig();
        this.sqlToRelConverterConfig = config.getSqlToRelConverterConfig();
        this.state = DtmCalcitePlannerImpl.State.STATE_0_CLOSED;
        this.traitDefs = config.getTraitDefs();
        this.convertletTable = config.getConvertletTable();
        this.executor = config.getExecutor();
        this.context = config.getContext();
        this.connectionConfig = this.connConfig();
        this.config = config.getSqlValidatorConfig();
        this.reset();
    }

    private CalciteConnectionConfig connConfig() {
        CalciteConnectionConfigImpl config = (CalciteConnectionConfigImpl) this.context.unwrap(CalciteConnectionConfigImpl.class);
        if (config == null) {
            config = new CalciteConnectionConfigImpl(new Properties());
        }

        if (!config.isSet(CalciteConnectionProperty.CASE_SENSITIVE)) {
            config = config.set(CalciteConnectionProperty.CASE_SENSITIVE, String.valueOf(this.parserConfig.caseSensitive()));
        }

        if (!config.isSet(CalciteConnectionProperty.CONFORMANCE)) {
            config = config.set(CalciteConnectionProperty.CONFORMANCE, String.valueOf(this.parserConfig.conformance()));
        }

        return config;
    }

    private void ensure(DtmCalcitePlannerImpl.State state) {
        if (state != this.state) {
            if (state.ordinal() < this.state.ordinal()) {
                throw new IllegalArgumentException("cannot move to " + state + " from " + this.state);
            } else {
                state.from(this);
            }
        }
    }

    public RelTraitSet getEmptyTraitSet() {
        return this.planner.emptyTraitSet();
    }

    public void close() {
        this.typeFactory = null;
        this.state = DtmCalcitePlannerImpl.State.STATE_0_CLOSED;
    }

    public void reset() {
        this.ensure(DtmCalcitePlannerImpl.State.STATE_0_CLOSED);
        this.state = DtmCalcitePlannerImpl.State.STATE_1_RESET;
    }

    private void ready() {
        if (this.state == State.STATE_0_CLOSED) {
            this.reset();
        }

        this.ensure(DtmCalcitePlannerImpl.State.STATE_1_RESET);
        RelDataTypeSystem typeSystem = (RelDataTypeSystem) this.connectionConfig.typeSystem(RelDataTypeSystem.class, new DtmRelDataTypeSystemImpl());
        this.typeFactory = new JavaTypeFactoryImpl(typeSystem);
        this.planner = new VolcanoPlanner(this.costFactory, this.context);
        RelOptUtil.registerDefaultRules(this.planner, this.connectionConfig.materializationsEnabled(), (Boolean) Hook.ENABLE_BINDABLE.get(false));
        this.planner.setExecutor(this.executor);
        this.state = DtmCalcitePlannerImpl.State.STATE_2_READY;
        if (this.traitDefs == null) {
            this.planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
            if ((Boolean) CalciteSystemProperty.ENABLE_COLLATION_TRAIT.value()) {
                this.planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
            }
        } else {
            UnmodifiableIterator<RelTraitDef> var2 = this.traitDefs.iterator();

            while (var2.hasNext()) {
                RelTraitDef def = var2.next();
                this.planner.addRelTraitDef(def);
            }
        }

    }

    public SqlNode parse(Reader reader) throws SqlParseException {
        if (this.state == State.STATE_0_CLOSED || this.state == State.STATE_1_RESET) {
            this.ready();
        }
        this.ensure(DtmCalcitePlannerImpl.State.STATE_2_READY);
        SqlParser parser = SqlParser.create(reader, this.parserConfig);
        SqlNode sqlNode = parser.parseStmt();
        this.state = DtmCalcitePlannerImpl.State.STATE_3_PARSED;
        return sqlNode;

    }

    public SqlNode validate(SqlNode sqlNode) throws ValidationException {
        this.ensure(DtmCalcitePlannerImpl.State.STATE_3_PARSED);
        this.validator = this.createSqlValidator(this.createCatalogReader());

        try {
            this.validatedSqlNode = this.validator.validate(sqlNode);
        } catch (RuntimeException var3) {
            throw new ValidationException(var3);
        }

        this.state = DtmCalcitePlannerImpl.State.STATE_4_VALIDATED;
        return this.validatedSqlNode;
    }

    private SqlConformance conformance() {
        return this.connectionConfig.conformance();
    }

    public Pair<SqlNode, RelDataType> validateAndGetType(SqlNode sqlNode) throws ValidationException {
        SqlNode validatedNode = this.validate(sqlNode);
        RelDataType type = this.validator.getValidatedNodeType(validatedNode);
        return Pair.of(validatedNode, type);
    }

    public final RelNode convert(SqlNode sql) throws RelConversionException {
        return this.rel(sql).rel;
    }

    public RelRoot rel(SqlNode sql) throws RelConversionException {
        this.ensure(DtmCalcitePlannerImpl.State.STATE_4_VALIDATED);

        assert this.validatedSqlNode != null;

        RexBuilder rexBuilder = this.createRexBuilder();
        RelOptCluster cluster = RelOptCluster.create(this.planner, rexBuilder);
        org.apache.calcite.sql2rel.SqlToRelConverter.Config config = SqlToRelConverter.configBuilder().withConfig(this.sqlToRelConverterConfig).withTrimUnusedFields(false).build();
        SqlToRelConverter sqlToRelConverter = new DtmSqlToRelConverter(this, this.validator, this.createCatalogReader(), cluster, this.convertletTable, config);
        this.root = sqlToRelConverter.convertQuery(this.validatedSqlNode, false, true);
        this.root = this.root.withRel(sqlToRelConverter.flattenTypes(this.root.rel, true));
        RelBuilder relBuilder = config.getRelBuilderFactory().create(cluster, (RelOptSchema) null);
        this.root = this.root.withRel(RelDecorrelator.decorrelateQuery(this.root.rel, relBuilder));
        this.state = DtmCalcitePlannerImpl.State.STATE_5_CONVERTED;
        return this.root;
    }

    public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
        if (this.planner == null) {
            this.ready();
        }

        SqlParser parser = SqlParser.create(queryString, this.parserConfig);

        SqlNode sqlNode;
        try {
            sqlNode = parser.parseQuery();
        } catch (SqlParseException var16) {
            throw new DtmException("parse failed", var16);
        }

        CalciteCatalogReader catalogReader = this.createCatalogReader().withSchemaPath(schemaPath);
        SqlValidator sqlValidator = this.createSqlValidator(catalogReader);
        RexBuilder rexBuilder = this.createRexBuilder();
        RelOptCluster cluster = RelOptCluster.create(this.planner, rexBuilder);
        org.apache.calcite.sql2rel.SqlToRelConverter.Config config = SqlToRelConverter.configBuilder().withConfig(this.sqlToRelConverterConfig).withTrimUnusedFields(false).build();
        SqlToRelConverter sqlToRelConverter = new DtmSqlToRelConverter(this, validator, catalogReader, cluster, this.convertletTable, config);
        RelRoot root1 = sqlToRelConverter.convertQuery(sqlNode, true, false);
        RelRoot root2 = root1.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
        RelBuilder relBuilder = config.getRelBuilderFactory().create(cluster, (RelOptSchema) null);
        return root2.withRel(RelDecorrelator.decorrelateQuery(root1.rel, relBuilder));
    }

    private CalciteCatalogReader createCatalogReader() {
        SchemaPlus rootSchema = rootSchema(this.defaultSchema);
        return new CalciteCatalogReader(CalciteSchema.from(rootSchema), CalciteSchema.from(this.defaultSchema).path((String) null), this.typeFactory, this.connectionConfig);
    }

    private SqlValidator createSqlValidator(CalciteCatalogReader catalogReader) {
        SqlConformance conformance = this.conformance();
        SqlOperatorTable opTab = ChainedSqlOperatorTable.of(new SqlOperatorTable[]{this.operatorTable, catalogReader});
        SqlValidator.Config configWithConformance = this.config.withSqlConformance(conformance);
        SqlValidator validator = new DtmCalciteSqlValidator(opTab, catalogReader, this.typeFactory, configWithConformance);
        validator.setIdentifierExpansion(true);
        return validator;
    }

    private static SchemaPlus rootSchema(SchemaPlus schema) {
        while (schema.getParentSchema() != null) {
            schema = schema.getParentSchema();
        }

        return schema;
    }

    private RexBuilder createRexBuilder() {
        return new RexBuilder(this.typeFactory);
    }

    public JavaTypeFactory getTypeFactory() {
        return this.typeFactory;
    }

    public RelNode transform(int ruleSetIndex, RelTraitSet requiredOutputTraits, RelNode rel) throws RelConversionException {
        this.ensure(DtmCalcitePlannerImpl.State.STATE_5_CONVERTED);
        rel.getCluster().setMetadataProvider(new CachingRelMetadataProvider(rel.getCluster().getMetadataProvider(), rel.getCluster().getPlanner()));
        Program program = (Program) this.programs.get(ruleSetIndex);
        return program.run(this.planner, rel, requiredOutputTraits, ImmutableList.of(), ImmutableList.of());
    }

    private static enum State {
        STATE_0_CLOSED {
            @Override
            void from(DtmCalcitePlannerImpl planner) {
                planner.close();
            }
        },
        STATE_1_RESET {
            @Override
            void from(DtmCalcitePlannerImpl planner) {
                planner.ensure(STATE_0_CLOSED);
                planner.reset();
            }
        },
        STATE_2_READY {
            @Override
            void from(DtmCalcitePlannerImpl planner) {
                STATE_1_RESET.from(planner);
                planner.ready();
            }
        },
        STATE_3_PARSED,
        STATE_4_VALIDATED,
        STATE_5_CONVERTED;

        private State() {
        }

        void from(DtmCalcitePlannerImpl planner) {
            throw new IllegalArgumentException("cannot move from " + planner.state + " to " + this);
        }
    }

    /**
     * @deprecated
     */
    @Deprecated
    public class ViewExpanderImpl implements RelOptTable.ViewExpander {
        ViewExpanderImpl() {
        }

        public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
            return DtmCalcitePlannerImpl.this.expandView(rowType, queryString, schemaPath, viewPath);
        }
    }
}

