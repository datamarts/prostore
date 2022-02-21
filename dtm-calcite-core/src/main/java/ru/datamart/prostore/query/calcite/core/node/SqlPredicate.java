package ru.datamart.prostore.query.calcite.core.node;

import org.apache.calcite.sql.SqlKind;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class SqlPredicate {
    private final SqlPredicateType type;
    private final SqlPredicatePart[] parts;
    private final List<SqlKind> kinds;

    private SqlPredicate(SqlPredicateType type, SqlPredicatePart[] parts) {
        this.type = type;
        this.parts = parts;
        this.kinds = Arrays.stream(parts).map(SqlPredicatePart::getSqlKind).filter(Objects::nonNull).collect(Collectors.toList());
    }

    public static SqlPredicate maybeOf(SqlPredicatePart... sqlKind) {
        return new SqlPredicate(SqlPredicateType.MAYBE, sqlKind);
    }

    public static SqlPredicate anyOf(SqlPredicatePart... sqlKind) {
        return new SqlPredicate(SqlPredicateType.ANY_OF, sqlKind);
    }

    public boolean test(int nodeKindIndex, SqlKindKey nodeKind) {
        for (SqlPredicatePart predicatePart : parts) {
            if ((predicatePart.getSqlKind() == null || predicatePart.getSqlKind() == nodeKind.getSqlKind()) &&
                    (predicatePart.isCanContainChildNum() || nodeKind.getChildNum() == null) &&
                    (!predicatePart.isFromStart() || nodeKindIndex == 0)) {
                return true;
            }
        }

        return false;
    }

    public List<SqlKind> getKinds() {
        return kinds;
    }

    public SqlPredicateType getType() {
        return type;
    }
}
