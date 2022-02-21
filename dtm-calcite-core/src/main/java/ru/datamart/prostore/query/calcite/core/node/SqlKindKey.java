package ru.datamart.prostore.query.calcite.core.node;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.calcite.sql.SqlKind;

@Getter
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class SqlKindKey {
    private final SqlKind sqlKind;
    private final Integer childNum;
}
