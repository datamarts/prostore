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
package ru.datamart.prostore.query.execution.plugin.adqm.mppr.shuttle;

import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;

public class SimpleQueryRelChecker extends RelHomogeneousShuttle {
    private final Checker checker = new Checker();
    private final RexChecker rexChecker = new RexChecker(checker);

    @Override
    public RelNode visit(RelNode other) {
        if (other instanceof TableScan) {
            checker.tablescans++;
        } else if (other instanceof Filter) {
            checker.filters++;
        } else if (!(other instanceof Project)) {
            checker.simple = false;
            return other;
        }

        other.accept(rexChecker);

        if (checker.tablescans > 1 || checker.filters > 1) {
            checker.simple = false;
        }

        if (!checker.simple) {
            return other;
        }

        return super.visit(other);
    }

    public boolean isSimple() {
        return checker.simple;
    }

    private static final class RexChecker extends RexShuttle {
        private final Checker checker;

        private RexChecker(Checker checker) {
            this.checker = checker;
        }

        @Override
        public RexNode visitSubQuery(RexSubQuery subQuery) {
            checker.simple = false;
            return subQuery;
        }
    }

    private static final class Checker {
        boolean simple = true;
        int tablescans;
        int filters;
    }
}
