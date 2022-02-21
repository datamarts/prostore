<#--

    Copyright © 2021 ProStore

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

-->
boolean IfNotExistsOpt() :
{
}
{
    <IF> <NOT> <EXISTS> { return true; }
|
    { return false; }
}

boolean IfExistsOpt() :
{
}
{
    <IF> <EXISTS> { return true; }
|
    { return false; }
}

boolean IsLogicalOnlyOpt() :
{
}
{
    <LOGICAL_ONLY> { return true; }
|
    { return false; }
}

SqlCreate SqlCreateSchema(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
}
{
    <SCHEMA> ifNotExists = IfNotExistsOpt() id = CompoundIdentifier()
    {
        return SqlDdlNodes.createSchema(s.end(this), replace, ifNotExists, id);
    }
}

SqlCreate SqlCreateForeignSchema(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
    SqlNode type = null;
    SqlNode library = null;
    SqlNodeList optionList = null;
}
{
    <FOREIGN> <SCHEMA> ifNotExists = IfNotExistsOpt() id = CompoundIdentifier()
    (
         <TYPE> type = StringLiteral()
    |
         <LIBRARY> library = StringLiteral()
    )
    [ optionList = Options() ]
    {
        return SqlDdlNodes.createForeignSchema(s.end(this), replace,
            ifNotExists, id, type, library, optionList);
    }
}

SqlNodeList Options() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <OPTIONS> { s = span(); } <LPAREN>
    [
        Option(list)
        (
            <COMMA>
            Option(list)
        )*
    ]
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void Option(List<SqlNode> list) :
{
    final SqlIdentifier id;
    final SqlNode value;
}
{
    id = SimpleIdentifier()
    value = Literal() {
        list.add(id);
        list.add(value);
    }
}

SqlNodeList TableElementList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    TableElement(list)
    (
        <COMMA> TableElement(list)
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void TableElement(List<SqlNode> list) :
{
    final SqlIdentifier id;
    final SqlDataTypeSpec type;
    final boolean nullable;
    final SqlNode e;
    final SqlNode constraint;
    SqlIdentifier name = null;
    final SqlNodeList columnList;
    final Span s = Span.of();
    final ColumnStrategy strategy;
}
{
    LOOKAHEAD(2) id = SimpleIdentifier()
    (
        type = DataType()
        nullable = NullableOptDefaultTrue()
        (
            [ <GENERATED> <ALWAYS> ] <AS> <LPAREN>
            e = Expression(ExprContext.ACCEPT_SUB_QUERY) <RPAREN>
            (
                <VIRTUAL> { strategy = ColumnStrategy.VIRTUAL; }
            |
                <STORED> { strategy = ColumnStrategy.STORED; }
            |
                { strategy = ColumnStrategy.VIRTUAL; }
            )
        |
            <DEFAULT_> e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
                strategy = ColumnStrategy.DEFAULT;
            }
        |
            {
                e = null;
                strategy = nullable ? ColumnStrategy.NULLABLE
                    : ColumnStrategy.NOT_NULLABLE;
            }
        )
        {
            list.add(
                SqlDdlNodes.column(s.add(id).end(this), id,
                    type.withNullable(nullable), e, strategy));
        }
    |
        { list.add(id); }
    )
|
    id = SimpleIdentifier() {
        list.add(id);
    }
|
    [ <CONSTRAINT> { s.add(this); } name = SimpleIdentifier() ]
    (
        <CHECK> { s.add(this); } <LPAREN>
        e = Expression(ExprContext.ACCEPT_SUB_QUERY) <RPAREN> {
            list.add(SqlDdlNodes.check(s.end(this), name, e));
        }
    |
        <UNIQUE> { s.add(this); }
        columnList = ParenthesizedSimpleIdentifierList() {
            list.add(SqlDdlNodes.unique(s.end(columnList), name, columnList));
        }
    |
        <PRIMARY>  { s.add(this); } <KEY>
        columnList = ParenthesizedSimpleIdentifierList() {
            list.add(SqlDdlNodes.primary(s.end(columnList), name, columnList));
        }
    )
}

SqlNodeList AttributeDefList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    AttributeDef(list)
    (
        <COMMA> AttributeDef(list)
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void AttributeDef(List<SqlNode> list) :
{
    final SqlIdentifier id;
    final SqlDataTypeSpec type;
    final boolean nullable;
    SqlNode e = null;
    final Span s = Span.of();
}
{
    id = SimpleIdentifier()
    (
        type = DataType()
        nullable = NullableOptDefaultTrue()
    )
    [ <DEFAULT_> e = Expression(ExprContext.ACCEPT_SUB_QUERY) ]
    {
        list.add(SqlDdlNodes.attribute(s.add(id).end(this), id,
            type.withNullable(nullable), e, null));
    }
}

SqlCreate SqlCreateType(Span s, boolean replace) :
{
    final SqlIdentifier id;
    SqlNodeList attributeDefList = null;
    SqlDataTypeSpec type = null;
}
{
    <TYPE>
    id = CompoundIdentifier()
    <AS>
    (
        attributeDefList = AttributeDefList()
    |
        type = DataType()
    )
    {
        return SqlDdlNodes.createType(s.end(this), replace, id, attributeDefList, type);
    }
}

SqlCreate SqlCreateTable(Span s, boolean replace) :
{
    final boolean ifNotExists;
    boolean isLogicalOnly;
    final SqlIdentifier id;
    SqlNodeList tableElementList = null;
    SqlNode query = null;
    SqlNodeList distributedBy = null;
    SqlNodeList destination = null;
}
{
    <TABLE> ifNotExists = IfNotExistsOpt() id = CompoundIdentifier()
    [ tableElementList = TableElementList() ]
    [ <DISTRIBUTED> <BY> distributedBy = ParenthesizedSimpleIdentifierList() ]
    [ <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) ]
    [ <DATASOURCE_TYPE> destination = ParenthesizedSimpleIdentifierList() ]
    isLogicalOnly = IsLogicalOnlyOpt()
    {
        return new SqlCreateTable(s.end(this), isLogicalOnly, ifNotExists, id,
            tableElementList, query, distributedBy, destination);
    }
}
SqlCreate SqlCreateDownloadExternalTable(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
    SqlNodeList tableElementList = null;
    SqlNode location = null;
    SqlNode format = null;
    SqlNode chunkSize = null;
}
{
    <DOWNLOAD> <EXTERNAL> <TABLE> ifNotExists = IfNotExistsOpt() id = CompoundIdentifier()
    [ tableElementList = TableElementList() ]
    <LOCATION> location = StringLiteral()
    <FORMAT> format = StringLiteral()
    [ <CHUNK_SIZE> chunkSize = NumericLiteral() ]
    {
        return new SqlCreateDownloadExternalTable(s.end(this),
            ifNotExists, id, tableElementList, location, format, chunkSize);
    }
}
SqlCreate SqlCreateUploadExternalTable(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
    SqlNodeList tableElementList = null;
    SqlNode location = null;
    SqlNode format = null;
    SqlNode messageLimit = null;
}
{
    <UPLOAD> <EXTERNAL> <TABLE> ifNotExists = IfNotExistsOpt() id = CompoundIdentifier()
    [ tableElementList = TableElementList() ]
    <LOCATION> location = StringLiteral()
    <FORMAT> format = StringLiteral()
    [ <MESSAGE_LIMIT> messageLimit = NumericLiteral() ]
    {
        return new SqlCreateUploadExternalTable(s.end(this),
            ifNotExists, id, tableElementList, location, format, messageLimit);
}
}
SqlCreate SqlCreateDatabase(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
    boolean isLogicalOnly;
}
{
    <DATABASE> ifNotExists = IfNotExistsOpt() id = CompoundIdentifier()
    isLogicalOnly = IsLogicalOnlyOpt()
    {
        return new SqlCreateDatabase(s.end(this), ifNotExists, id, isLogicalOnly);
    }
}
SqlCreate SqlCreateView(Span s, boolean replace) :
{
    final SqlIdentifier id;
    SqlNodeList columnList = null;
    final SqlNode query;
}
{
    <VIEW> id = CompoundIdentifier()
    [ columnList = ParenthesizedSimpleIdentifierList() ]
    <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) {
        return new SqlCreateView(s.end(this), replace, id, columnList, query);
    }
}
SqlAlter SqlAlterView(Span s) :
{
    final SqlIdentifier id;
    SqlNodeList columnList = null;
    final SqlNode query;
}
{
    <VIEW> id = CompoundIdentifier()
    [ columnList = ParenthesizedSimpleIdentifierList() ]
    <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) {
        return new SqlAlterView(s.end(this), id, columnList, query);
}
}
SqlCreate SqlCreateMaterializedView(Span s, boolean replace) :
{
    final SqlIdentifier id;
    SqlNodeList tableElementList = null;
    final SqlNode query;
    SqlNodeList distributedBy = null;
    SqlNodeList destination = null;
    boolean isLogicalOnly;
}
{
    <MATERIALIZED> <VIEW> id = CompoundIdentifier()
    [ tableElementList = TableElementList() ]
    [ <DISTRIBUTED> <BY> distributedBy = ParenthesizedSimpleIdentifierList() ]
    [ <DATASOURCE_TYPE> destination = ParenthesizedSimpleIdentifierList() ]
    <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    isLogicalOnly = IsLogicalOnlyOpt()
    {
        return new SqlCreateMaterializedView(s.end(this), id, tableElementList, distributedBy, destination, query, isLogicalOnly);
    }
}
SqlNode SqlBeginDelta() :
{
    SqlParserPos beginPos;
    SqlNode num = null;
}
{
    <BEGIN> <DELTA>
    {
    beginPos = getPos();
    }
    [ <SET> num = NumericLiteral() ]
{
    return new SqlBeginDelta(beginPos, num);
}
}
SqlNode SqlCommitDelta() :
{
    SqlParserPos commitPos;
    SqlNode dateTime = null;
}
{
    <COMMIT> <DELTA>
    {
    commitPos = getPos();
    }
    [ <SET> dateTime = StringLiteral() ]
{
    return new SqlCommitDelta(commitPos, dateTime);
}
}
SqlNode SqlRollbackDelta() :
{
    SqlParserPos rollbackPos;
}
{
    <ROLLBACK> <DELTA>
    {
            rollbackPos = getPos();
    }
{
return new SqlRollbackDelta(rollbackPos);
}
}
SqlNode GetWriteOperations() :
{
    SqlParserPos parserPos;
}
{
    <GET_WRITE_OPERATIONS>
    {
        parserPos = getPos();
    }
    <LPAREN><RPAREN>
    {
        return new GetWriteOperations(parserPos);
    }
}
SqlNode ResumeWriteOperation() :
{
    SqlParserPos parserPos;
    SqlNode writeOperationNumber = null;
}
{
    <RESUME_WRITE_OPERATION>
    {
        parserPos = getPos();
    }
    <LPAREN>
        [ writeOperationNumber = NumericLiteral() ]
    <RPAREN>
    {
        return new ResumeWriteOperation(parserPos, writeOperationNumber);
    }
}
SqlNode SqlConfigShow() :
{
    SqlParserPos parserPos;
    SqlNode parameterName = null;
}
{
    <CONFIG_SHOW>
    {
        parserPos = getPos();
    }
    <LPAREN>
        [ parameterName = StringLiteral() ]
    <RPAREN>
    {
        return new ru.datamart.prostore.query.calcite.core.extension.config.function.SqlConfigShow(parserPos, parameterName);
    }
}
SqlNode SqlAllowChanges() :
{
    SqlParserPos parserPos;
    SqlNode datamart = null;
    SqlNode denyCode = null;
}
{
    <ALLOW_CHANGES>
    {
        parserPos = getPos();
    }
    <LPAREN>
        (
            datamart = SimpleIdentifier()
            [<COMMA> denyCode = StringLiteral()]
        |
            [denyCode = StringLiteral()]
        |
            {
                datamart = null;
                denyCode = null;
            }
        )
    <RPAREN>
    {
        return new ru.datamart.prostore.query.calcite.core.extension.ddl.SqlAllowChanges(parserPos, datamart, denyCode);
    }
}
SqlNode SqlDenyChanges() :
{
    SqlParserPos parserPos;
    SqlNode datamart = null;
    SqlNode denyCode = null;
}
{
    <DENY_CHANGES>
    {
        parserPos = getPos();
    }
    <LPAREN>
        (
            datamart = SimpleIdentifier()
            [<COMMA> denyCode = StringLiteral()]
        |
            [denyCode = StringLiteral()]
        |
            {
                datamart = null;
                denyCode = null;
            }
        )
    <RPAREN>
    {
        return new ru.datamart.prostore.query.calcite.core.extension.ddl.SqlDenyChanges(parserPos, datamart, denyCode);
    }
}
SqlNode SqlGetChanges() :
{
    SqlParserPos parserPos;
    SqlIdentifier datamart = null;
}
{
    <GET_CHANGES>
    {
        parserPos = getPos();
    }
    <LPAREN>
        (
            [datamart = SimpleIdentifier()]
        )
    <RPAREN>
    {
        return new ru.datamart.prostore.query.calcite.core.extension.check.SqlGetChanges(parserPos, datamart);
    }
}
private void FunctionJarDef(SqlNodeList usingList) :
{
    final SqlDdlNodes.FileType fileType;
    final SqlNode uri;
}
{
    (
        <ARCHIVE> { fileType = SqlDdlNodes.FileType.ARCHIVE; }
    |
        <FILE> { fileType = SqlDdlNodes.FileType.FILE; }
    |
        <JAR> { fileType = SqlDdlNodes.FileType.JAR; }
    ) {
        usingList.add(SqlLiteral.createSymbol(fileType, getPos()));
    }
    uri = StringLiteral() {
        usingList.add(uri);
    }
}

SqlCreate SqlCreateFunction(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
    final SqlNode className;
    SqlNodeList usingList = SqlNodeList.EMPTY;
}
{
    <FUNCTION> ifNotExists = IfNotExistsOpt()
    id = CompoundIdentifier()
    <AS>
    className = StringLiteral()
    [
        <USING> {
            usingList = new SqlNodeList(getPos());
        }
        FunctionJarDef(usingList)
        (
            <COMMA>
            FunctionJarDef(usingList)
        )*
    ] {
        return SqlDdlNodes.createFunction(s.end(this), replace, ifNotExists,
            id, className, usingList);
    }
}

SqlDrop SqlDropSchema(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
    final boolean foreign;
}
{
    (
        <FOREIGN> { foreign = true; }
    |
        { foreign = false; }
    )
    <SCHEMA> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropSchema(s.end(this), foreign, ifExists, id);
    }
}

SqlDrop SqlDropType(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <TYPE> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropType(s.end(this), ifExists, id);
    }
}

SqlDrop SqlDropTable(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
    SqlNode destination = null;
    boolean isLogicalOnly;
}
{
    <TABLE> ifExists = IfExistsOpt() id = CompoundIdentifier()
    [ <DATASOURCE_TYPE> destination = DatasourceTypeIdentifier() ]
    isLogicalOnly = IsLogicalOnlyOpt()
    {
        return new SqlDropTable(s.end(this), ifExists, id, destination, isLogicalOnly);
    }
}
SqlDrop DropDatabase(Span s, boolean replace) :
{
   final boolean ifExists;
   final SqlIdentifier id;
   boolean isLogicalOnly;
}
{
   <DATABASE> ifExists = IfExistsOpt() id = CompoundIdentifier()
   isLogicalOnly = IsLogicalOnlyOpt()
   {
      return new DropDatabase(s.end(this), ifExists, id, isLogicalOnly);
   }
}
SqlDrop SqlDropUploadExternalTable(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <UPLOAD> <EXTERNAL> <TABLE> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return new SqlDropUploadExternalTable(s.end(this), ifExists, id);
    }
}
SqlDrop SqlDropDownloadExternalTable(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <DOWNLOAD> <EXTERNAL> <TABLE> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return new SqlDropDownloadExternalTable(s.end(this), ifExists, id);
}
}
SqlDrop SqlDropView(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <VIEW> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropView(s.end(this), ifExists, id);
    }
}

SqlDrop SqlDropMaterializedView(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
    SqlNode destination = null;
    boolean isLogicalOnly;
}
{
    <MATERIALIZED> <VIEW> ifExists = IfExistsOpt() id = CompoundIdentifier()
    [ <DATASOURCE_TYPE> destination = DatasourceTypeIdentifier() ]
    isLogicalOnly = IsLogicalOnlyOpt()
    {
        return new SqlDropMaterializedView(s.end(this), ifExists, id, destination, isLogicalOnly);
    }
}

SqlDrop SqlDropFunction(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <FUNCTION> ifExists = IfExistsOpt()
    id = CompoundIdentifier() {
        return SqlDdlNodes.dropFunction(s.end(this), ifExists, id);
    }
}
SqlNode SqlUseSchema() :
{
    SqlParserPos pos;
    final SqlIdentifier id;
}
{
    <USE> id = CompoundIdentifier()
{
        pos = getPos();
}
    {
        return new ru.datamart.prostore.query.calcite.core.extension.dml.SqlUseSchema(pos, id);
    }
}
SqlNode SqlGetDeltaOk() :
{
    SqlParserPos pos;
}
{
    <GET_DELTA_OK>
    {
        pos = getPos();
    }
    <LPAREN> <RPAREN>
    {
        return new ru.datamart.prostore.query.calcite.core.extension.delta.function.SqlGetDeltaOk(pos);
    }
}
SqlNode SqlGetDeltaHot() :
{
    SqlParserPos pos;
}
{
    <GET_DELTA_HOT>
    {
        pos = getPos();
    }
    <LPAREN> <RPAREN>
    {
        return new ru.datamart.prostore.query.calcite.core.extension.delta.function.SqlGetDeltaHot(pos);
    }
}
SqlNode SqlGetDeltaByDateTime() :
{
    Span s;
    SqlNode deltaDateTime = null;
}
{
    <GET_DELTA_BY_DATETIME>
    {
        s = span();
    }
    <LPAREN>
        deltaDateTime = StringLiteral()
    <RPAREN>
    {
        return new ru.datamart.prostore.query.calcite.core.extension.delta.function.SqlGetDeltaByDateTime(s.end(this), deltaDateTime);
    }
}
SqlNode SqlGetDeltaByNum() :
{
    Span s;
    SqlNode deltaNum = null;
}
{
    <GET_DELTA_BY_NUM>
    {
        s = span();
    }
    <LPAREN>
        deltaNum = NumericLiteral()
    <RPAREN>
    {
        return new ru.datamart.prostore.query.calcite.core.extension.delta.function.SqlGetDeltaByNum(s.end(this), deltaNum);
    }
}
SqlNode SqlConfigStorageAdd() :
{
    Span s;
    SqlNode sourceType = null;
}
{
    <CONFIG_STORAGE_ADD>
    {
        s = span();
    }
    <LPAREN>
        sourceType = StringLiteral()
    <RPAREN>
    {
        return new ru.datamart.prostore.query.calcite.core.extension.config.function.SqlConfigStorageAdd(s.end(this), sourceType);
    }
}
SqlNode SqlCheckDatabase() :
{
    Span s;
    SqlIdentifier id = null;
}
{
    <CHECK_DATABASE>
    {
        s = span();
    }
    (
        <LPAREN> [id = CompoundIdentifier()] <RPAREN>
        |
        {id = null;}
    )
    {
        return new ru.datamart.prostore.query.calcite.core.extension.check.SqlCheckDatabase(s.end(this), id);
    }
}
SqlNode SqlCheckTable() :
{
    Span s;
    final SqlIdentifier id;
}
{
    <CHECK_TABLE>
    {
        s = span();
    }
    <LPAREN> id = CompoundIdentifier() <RPAREN>
    {
        return new ru.datamart.prostore.query.calcite.core.extension.check.SqlCheckTable(s.end(this), id);
    }
}

SqlNode SqlCheckMaterializedView() :
{
    Span s;
    SqlIdentifier id = null;
}
{
<CHECK_MATERIALIZED_VIEW>
    {
    s = span();
    }
    <LPAREN> [id = CompoundIdentifier()] <RPAREN>
    {
        return new ru.datamart.prostore.query.calcite.core.extension.check.SqlCheckMaterializedView(s.end(this), id);
    }
}

SqlNode SqlCheckData() :
{
    Span s;
    final SqlIdentifier id;
    SqlLiteral deltaNum = null;
    SqlLiteral normalization = null;
    List<SqlNode> tableElementList = null;
}
{
    <CHECK_DATA>
    {
        s = span();
    }
    <LPAREN> id = CompoundIdentifier()
    <COMMA>
        deltaNum = NumericLiteral()
    [ <COMMA> normalization = NumericLiteral() ]
    [ <COMMA> <LBRACKET> tableElementList = SelectList() <RBRACKET> ]
    <RPAREN>
    {
        return new ru.datamart.prostore.query.calcite.core.extension.check.SqlCheckData(s.end(this), id, deltaNum, normalization, tableElementList);
    }
}
SqlNode SqlCheckVersions() :
{
    SqlParserPos pos;
}
{
    <CHECK_VERSIONS>
    {
        pos = getPos();
    }
    <LPAREN> <RPAREN>
    {
        return new ru.datamart.prostore.query.calcite.core.extension.check.SqlCheckVersions(pos);
    }
}
SqlNode SqlTruncateHistory() :
{
    final Span s;
    final SqlIdentifier id;
    final SqlNode datetime;
    SqlNode conditions = null;
}
{
    <TRUNCATE> <HISTORY>
    {
            s = span();
    }
    id = CompoundIdentifier()
    <FOR> <SYSTEM_TIME> <AS> <OF> datetime = StringLiteral()
    [<WHERE> conditions = SqlExpressionEof()]
    {
        return new ru.datamart.prostore.query.calcite.core.extension.ddl.truncate.SqlTruncateHistory(s.end(this), id, datetime, conditions);
    }
}
SqlNode SqlRollbackCrashedWriteOps() :
{
    SqlParserPos pos;
}
{
    <ROLLBACK> <CRASHED_WRITE_OPERATIONS>
    {
            pos = getPos();
    }
    {
        return new ru.datamart.prostore.query.calcite.core.extension.edml.SqlRollbackCrashedWriteOps(pos);
    }
}
SqlNode SqlCheckSum() :
{
    Span s;
    final SqlLiteral deltaNum;
    SqlLiteral normalization = null;
    SqlIdentifier table = null;
    List<SqlNode> tableElementList = null;
}
{
    <CHECK_SUM>
    {
        s = span();
    }
    <LPAREN> deltaNum = NumericLiteral()
    [ <COMMA> normalization = NumericLiteral() ]
    [ <COMMA>
        table = CompoundIdentifier()
        [ <COMMA> <LBRACKET> tableElementList = SelectList() <RBRACKET> ]
    ]
    <RPAREN>
    {
        return new ru.datamart.prostore.query.calcite.core.extension.check.SqlCheckSumBasic(s.end(this), deltaNum, normalization, table, tableElementList);
    }
}

SqlNode SqlCheckSumSnapshot() :
{
    Span s;
    final SqlLiteral deltaNum;
    SqlLiteral normalization = null;
    SqlIdentifier table = null;
    List<SqlNode> tableElementList = null;
}
{
    <CHECK_SUM_SNAPSHOT>
    {
        s = span();
    }
    <LPAREN> deltaNum = NumericLiteral()
    [ <COMMA> normalization = NumericLiteral() ]
    [ <COMMA>
        table = CompoundIdentifier()
        [ <COMMA> <LBRACKET> tableElementList = SelectList() <RBRACKET> ]
    ]
    <RPAREN>
    {
        return new ru.datamart.prostore.query.calcite.core.extension.check.SqlCheckSumSnapshot(s.end(this), deltaNum, normalization, table, tableElementList);
    }
}
SqlNode SqlGetEntityDdl() :
{
    Span s;
    SqlIdentifier id = null;
}
{
<GET_ENTITY_DDL>
{
    s = span();
}
<LPAREN>  id = CompoundIdentifier()
    <RPAREN>
    {
        return new ru.datamart.prostore.query.calcite.core.extension.check.SqlGetEntityDdl(s.end(this), id);
    }
}