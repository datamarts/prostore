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
package ru.datamart.prostore.calcite.adqm.configuration;

import ru.datamart.prostore.calcite.adqm.extension.parser.SqlEddlParserImpl;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.util.SourceStringReader;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AdqmCalciteConfiguration {

    public SqlParserImplFactory eddlParserImplFactory() {
        return reader -> {
            final SqlEddlParserImpl parser = new SqlEddlParserImpl(reader);
            if (reader instanceof SourceStringReader) {
                final String sql = ((SourceStringReader) reader).getSourceString();
                parser.setOriginalSql(sql);
            }
            return parser;
        };
    }
}
