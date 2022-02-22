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
package ru.datamart.prostore.query.execution.plugin.adp.base.factory.hash;

public class AdpFunctionFactory {

    private static final String CREATE_OR_REPLACE_FUNC = "CREATE OR REPLACE FUNCTION dtmInt32Hash(bytea) RETURNS integer\n" +
            "    AS 'select get_byte($1, 0)+(get_byte($1, 1)<<8)+(get_byte($1, 2)<<16)+(get_byte($1, 3)<<24)' \n" +
            "    LANGUAGE SQL\n" +
            "    IMMUTABLE\n" +
            "    LEAKPROOF\n" +
            "    RETURNS NULL ON NULL INPUT;";

    public static String createInt32HashFunction() {
        return CREATE_OR_REPLACE_FUNC;
    }
}
