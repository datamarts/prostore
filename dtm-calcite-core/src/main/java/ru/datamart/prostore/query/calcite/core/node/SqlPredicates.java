package ru.datamart.prostore.query.calcite.core.node;

import lombok.Getter;
import lombok.val;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

@Getter
public class SqlPredicates {
    private final List<SqlPredicate> predicates;
    private final List<SqlKind> initialKinds;
    private final int notMaybePredicates;

    private SqlPredicates(List<SqlPredicate> predicates) {
        this.predicates = new ArrayList<>(predicates);
        this.notMaybePredicates = (int) predicates.stream().filter(sqlPredicate -> sqlPredicate.getType() != SqlPredicateType.MAYBE).count();

        val initialKinds = new HashSet<SqlKind>();
        for (int i = predicates.size() - 1; i >= 0; i--) {
            val predicate = predicates.get(i);
            initialKinds.addAll(predicate.getKinds());
            if (predicate.getType() == SqlPredicateType.ANY_OF) {
                break;
            }
        }
        this.initialKinds = new ArrayList<>(initialKinds);
    }

    public static Builder builder() {
        return new Builder();
    }

    public boolean isEmpty() {
        return predicates.isEmpty();
    }

    public int size() {
        return predicates.size();
    }

    public SqlPredicate get(int idx) {
        return predicates.get(idx);
    }

    public List<SqlKind> getInitialKinds() {
        return initialKinds;
    }

    public static class Builder {
        private final List<SqlPredicate> predicates = new ArrayList<>();

        public Builder anyOf(SqlPredicatePart... parts) {
            predicates.add(SqlPredicate.anyOf(parts));
            return this;
        }

        public Builder maybeOf(SqlPredicatePart... parts) {
            predicates.add(SqlPredicate.maybeOf(parts));
            return this;
        }

        public SqlPredicates build() {
            return new SqlPredicates(predicates);
        }
    }
}
