package ru.datamart.prostore.query.calcite.core.node;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.apache.calcite.sql.SqlNode;

import java.util.function.BiConsumer;

@Data
@Builder(toBuilder = true)
@AllArgsConstructor
public class SqlNodeConsumer<T extends SqlNode> {
    private final BiConsumer<T, SqlNode> consumer;
    private final SqlNode parentNode;

    public SqlNodeConsumer(T parentNode, BiConsumer<T, SqlNode> consumer) {
        this.parentNode = parentNode;
        this.consumer = consumer;
    }

    @SuppressWarnings("unchecked")
    public void accept(SqlNode sqlNode) {
        consumer.accept((T) parentNode, sqlNode);
    }
}
