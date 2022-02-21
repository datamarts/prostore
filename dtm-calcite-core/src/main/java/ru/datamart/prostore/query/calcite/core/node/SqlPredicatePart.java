package ru.datamart.prostore.query.calcite.core.node;

import lombok.Getter;
import org.apache.calcite.sql.SqlKind;

@Getter
public class SqlPredicatePart {
    private final SqlKind sqlKind;
    private final boolean canContainChildNum;
    private final boolean fromStart;

    private SqlPredicatePart(SqlKind sqlKind, boolean canContainChildNum, boolean fromStart) {
        this.sqlKind = sqlKind;
        this.canContainChildNum = canContainChildNum;
        this.fromStart = fromStart;
    }

    public static SqlPredicatePart anyFromStart() {
        return new SqlPredicatePart(null, false, true);
    }

    public static SqlPredicatePart eqFromStart(SqlKind sqlKind) {
        return new SqlPredicatePart(sqlKind, false, true);
    }

    public static SqlPredicatePart eqWithNum(SqlKind sqlKind) {
        return new SqlPredicatePart(sqlKind, true, false);
    }

    public static SqlPredicatePart eq(SqlKind sqlKind) {
        return new SqlPredicatePart(sqlKind, false, false);
    }
}
