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
package org.apache.calcite.sql.fun;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.validate.SqlValidator;

public class SqlCoalesceFunction extends SqlFunction {
    public SqlCoalesceFunction() {
        super("COALESCE",
                SqlKind.COALESCE,
                ReturnTypes.cascade(ReturnTypes.LEAST_RESTRICTIVE,
                        SqlTypeTransforms.LEAST_NULLABLE),
                null,
                OperandTypes.SAME_VARIADIC,
                SqlFunctionCategory.SYSTEM);
    }

    @Override
    public SqlNode rewriteCall(SqlValidator validator, SqlCall call) {
        //hint: reworked to disable calcite coalesce rewrite
        return call;
    }
}
