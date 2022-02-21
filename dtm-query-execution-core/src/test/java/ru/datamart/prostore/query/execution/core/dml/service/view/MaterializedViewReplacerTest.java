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
package ru.datamart.prostore.query.execution.core.dml.service.view;

import ru.datamart.prostore.common.delta.DeltaInformation;
import ru.datamart.prostore.common.delta.DeltaType;
import ru.datamart.prostore.common.delta.SelectOnInterval;
import ru.datamart.prostore.common.exception.DeltaRangeInvalidException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.service.DefinitionService;
import ru.datamart.prostore.query.calcite.core.util.CalciteUtil;
import ru.datamart.prostore.query.execution.core.base.service.delta.DeltaInformationExtractor;
import ru.datamart.prostore.query.execution.core.base.service.delta.DeltaInformationService;
import ru.datamart.prostore.query.execution.core.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.core.calcite.service.CoreCalciteDefinitionService;
import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MaterializedViewReplacerTest {

    private final CalciteConfiguration config = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();

    @Spy
    private final DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(config.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory()));

    @Mock
    private DeltaInformationExtractor deltaInformationExtractor;

    @Mock
    private DeltaInformationService deltaInformationService;

    @InjectMocks
    private MaterializedViewReplacer viewReplacer;

    @Mock
    private ViewReplacerService viewReplacerService;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testViewNotReplacedWhenNoHints() {
        ViewReplaceContext context = ViewReplaceContext.builder()
                .viewReplacerService(viewReplacerService)
                .build();

        val deltaInformation = new DeltaInformation(
                "",
                null,
                false,
                DeltaType.WITHOUT_SNAPSHOT,
                null,
                null,
                "datamart",
                "mat_view",
                null
        );

        when(deltaInformationExtractor.getDeltaInformation(any(), any()))
                .thenReturn(deltaInformation);

        viewReplacer.replace(context)
                .onComplete(result -> {
                    verify(viewReplacerService, never()).replace(any());
                })
                .onFailure(result -> Assert.fail("Materialized view should not have been replaced"));
    }

    @Test
    public void testViewReplacedByDateTime() {
        ViewReplaceContext context = ViewReplaceContext.builder()
                .datamart("datamart")
                .viewReplacerService(viewReplacerService)
                .entity(new Entity().toBuilder()
                        .viewQuery("SELECT * FROM datamart.some_table")
                        .materializedDeltaNum(1L)
                        .build())
                .build();

        val deltaInformation = new DeltaInformation(
                "",
                "'2021-06-22 11:42:00'",
                false,
                DeltaType.DATETIME,
                null,
                null,
                "datamart",
                "mat_view",
                null
        );

        when(deltaInformationExtractor.getDeltaInformation(any(), any()))
                .thenReturn(deltaInformation);
        // Delta of mat view (1) is less than delta from the request (5). Replacing a view
        doAnswer(answer -> Future.succeededFuture(5L)).when(deltaInformationService).getDeltaNumByDatetime(context.getDatamart(), CalciteUtil.parseLocalDateTime("2021-06-22 11:42:00"));
        doAnswer(answer -> Future.succeededFuture()).when(viewReplacerService).replace(any(ViewReplaceContext.class));
        viewReplacer.replace(context)
                .onComplete(result -> {
                    verify(viewReplacerService).replace(any(ViewReplaceContext.class));
                    SqlNode viewQueryNode = context.getViewQueryNode();
                    assertThat(viewQueryNode.toString()).isEqualToNormalizingNewlines("SELECT *\nFROM `datamart`.`some_table`");
                })
                .onFailure(result -> Assert.fail("Error while replacing materialized view"));
    }

    @Test
    public void testViewNotReplacedByDateTime() {
        ViewReplaceContext context = ViewReplaceContext.builder()
                .datamart("datamart")
                .viewReplacerService(viewReplacerService)
                .entity(new Entity().toBuilder()
                        .viewQuery("SELECT * FROM datamart.some_table")
                        .materializedDeltaNum(5L)
                        .build())
                .build();

        val deltaInformation = new DeltaInformation(
                "",
                "'2021-06-22 11:42:00'",
                false,
                DeltaType.DATETIME,
                null,
                null,
                "datamart",
                "mat_view",
                null
        );

        when(deltaInformationExtractor.getDeltaInformation(any(), any()))
                .thenReturn(deltaInformation);
        // Delta of mat view (5) is greater or equal than delta from the request (5). NOT replacing a view
        doAnswer(answer -> Future.succeededFuture(5L)).when(deltaInformationService).getDeltaNumByDatetime(context.getDatamart(), CalciteUtil.parseLocalDateTime("2021-06-22 11:42:00"));
        doAnswer(answer -> Future.succeededFuture()).when(viewReplacerService).replace(any(ViewReplaceContext.class));
        viewReplacer.replace(context)
                .onComplete(result -> {
                    verify(viewReplacerService, never()).replace(any(ViewReplaceContext.class));
                })
                .onFailure(result -> Assert.fail("Error while replacing materialized view"));
    }

    @Test
    public void testViewReplacedByDeltaNum() {
        ViewReplaceContext context = ViewReplaceContext.builder()
                .datamart("datamart")
                .viewReplacerService(viewReplacerService)
                .entity(new Entity().toBuilder()
                        .viewQuery("SELECT * FROM datamart.some_table")
                        .materializedDeltaNum(1L)
                        .build())
                .build();

        // Delta of mat view (1) is less than delta from the request (5). Replacing a view
        val deltaInformation = new DeltaInformation(
                "",
                null,
                false,
                DeltaType.NUM,
                5L,
                null,
                "datamart",
                "mat_view",
                null
        );

        when(deltaInformationExtractor.getDeltaInformation(any(), any()))
                .thenReturn(deltaInformation);
        doAnswer(answer -> Future.succeededFuture()).when(viewReplacerService).replace(any(ViewReplaceContext.class));

        viewReplacer.replace(context)
                .onComplete(result -> {
                    verify(viewReplacerService).replace(any(ViewReplaceContext.class));
                    SqlNode viewQueryNode = context.getViewQueryNode();
                    assertThat(viewQueryNode.toString()).isEqualToNormalizingNewlines("SELECT *\nFROM `datamart`.`some_table`");
                })
                .onFailure(result -> Assert.fail("Error while replacing materialized view"));
    }

    @Test
    public void testViewNotReplacedByDeltaNum() {
        ViewReplaceContext context = ViewReplaceContext.builder()
                .datamart("datamart")
                .viewReplacerService(viewReplacerService)
                .entity(new Entity().toBuilder()
                        .materializedDeltaNum(10L)
                        .build())
                .build();

        // Delta of mat view (10) is greater than delta from the request (5). NOT replacing a view
        val deltaInformation = new DeltaInformation(
                "",
                null,
                false,
                DeltaType.NUM,
                5L,
                null,
                "datamart",
                "mat_view",
                null
        );

        when(deltaInformationExtractor.getDeltaInformation(any(), any()))
                .thenReturn(deltaInformation);

        viewReplacer.replace(context)
                .onComplete(result -> {
                    verify(viewReplacerService, never()).replace(any());
                })
                .onFailure(result -> Assert.fail("Error while replacing materialized view"));
    }

    @Test
    public void testLatestUncommittedDeltaIsNotSupported() {
        ViewReplaceContext context = ViewReplaceContext.builder()
                .viewReplacerService(viewReplacerService)
                .build();

        val deltaInformation = new DeltaInformation(
                "",
                null,
                true,
                DeltaType.NUM,
                null,
                null,
                "datamart",
                "mat_view",
                null
        );

        when(deltaInformationExtractor.getDeltaInformation(any(), any()))
                .thenReturn(deltaInformation);

        boolean thrown = false;
        try {
            viewReplacer.replace(context).onComplete(result -> {});
            fail("Should not reach this line");
        } catch (DeltaRangeInvalidException e) {
            thrown = true;
            verify(viewReplacerService, never()).replace(any());
        }

        if (!thrown) {
            fail("DeltaRangeInvalidException was expected");
        }
    }

    @Test
    public void testViewNotReplacedForStartedIn() {
        ViewReplaceContext context = ViewReplaceContext.builder()
                .datamart("datamart")
                .viewReplacerService(viewReplacerService)
                .entity(new Entity().toBuilder()
                        .materializedDeltaNum(5L)
                        .build())
                .build();

        val deltaInformation = new DeltaInformation(
                "",
                null,
                false,
                DeltaType.STARTED_IN,
                null,
                new SelectOnInterval(1L, 2L),
                "datamart",
                "mat_view",
                null
        );

        when(deltaInformationExtractor.getDeltaInformation(any(), any()))
                .thenReturn(deltaInformation);

        viewReplacer.replace(context)
                .onComplete(result -> {
                    // NOT replacing because mat view delta_num (5) is greater than the interval (1, 2)
                    verify(viewReplacerService, never()).replace(any());
                })
                .onFailure(result -> Assert.fail("Error while replacing materialized view"));
    }

    @Test
    public void testExceptionThrownForStartedIn() {
        ViewReplaceContext context = ViewReplaceContext.builder()
                .datamart("datamart")
                .viewReplacerService(viewReplacerService)
                .entity(new Entity().toBuilder()
                        .materializedDeltaNum(5L)
                        .build())
                .build();

        val deltaInformation = new DeltaInformation(
                "",
                null,
                false,
                DeltaType.STARTED_IN,
                null,
                new SelectOnInterval(10L, 20L),
                "datamart",
                "mat_view",
                null
        );

        when(deltaInformationExtractor.getDeltaInformation(any(), any()))
                .thenReturn(deltaInformation);

        boolean thrown = false;
        try {
            viewReplacer.replace(context).onComplete(result -> {});
            fail("Should not reach this line");
        } catch (DeltaRangeInvalidException e) {
            thrown = true;
            verify(viewReplacerService, never()).replace(any());
        }

        if (!thrown) {
            fail("DeltaRangeInvalidException was expected");
        }
    }

    @Test
    public void testViewNotReplacedForFinishedIn() {
        ViewReplaceContext context = ViewReplaceContext.builder()
                .datamart("datamart")
                .viewReplacerService(viewReplacerService)
                .entity(new Entity().toBuilder()
                        .materializedDeltaNum(5L)
                        .build())
                .build();

        val deltaInformation = new DeltaInformation(
                "",
                null,
                false,
                DeltaType.FINISHED_IN,
                null,
                new SelectOnInterval(1L, 2L),
                "datamart",
                "mat_view",
                null
        );

        when(deltaInformationExtractor.getDeltaInformation(any(), any()))
                .thenReturn(deltaInformation);
        viewReplacer.replace(context)
                .onComplete(result -> {
                    // NOT replacing because mat view delta_num (5) is greater than the interval (1, 2)
                    verify(viewReplacerService, never()).replace(any());
                })
                .onFailure(result -> Assert.fail("Error while replacing materialized view"));
    }

    @Test
    public void testExceptionThrownForFinishedIn() {
        ViewReplaceContext context = ViewReplaceContext.builder()
                .datamart("datamart")
                .viewReplacerService(viewReplacerService)
                .entity(new Entity().toBuilder()
                        .materializedDeltaNum(5L)
                        .build())
                .build();

        val deltaInformation = new DeltaInformation(
                "",
                null,
                false,
                DeltaType.FINISHED_IN,
                null,
                new SelectOnInterval(10L, 20L),
                "datamart",
                "mat_view",
                null
        );

        when(deltaInformationExtractor.getDeltaInformation(any(), any()))
                .thenReturn(deltaInformation);

        boolean thrown = false;
        try {
            viewReplacer.replace(context).onComplete(result -> {});
            fail("Should not reach this line");
        } catch (DeltaRangeInvalidException e) {
            thrown = true;
            verify(viewReplacerService, never()).replace(any());
        }

        if (!thrown) {
            fail("DeltaRangeInvalidException was expected");
        }
    }

}
