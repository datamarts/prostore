package ru.datamart.prostore.query.calcite.core.node;

import lombok.*;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSnapshot;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Data
@RequiredArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class SqlTreeNode implements Comparable<SqlTreeNode> {
    private final int id;
    private final int parentId;
    private final int level;
    private final SqlNode node;
    private final SqlNodeConsumer<? extends SqlNode> sqlNodeSetter;
    private final List<SqlKindKey> kindPath;
    private boolean identifier;
    private int childPos;

    public Optional<SqlTreeNode> createChild(int id, SqlNode node, SqlNodeConsumer<? extends SqlNode> sqlNodeSetter) {
        if (node == null) return Optional.empty();
        List<SqlKindKey> kindKeyPath = getKindKeyPath(node);
        childPos++;
        return Optional.of(
                new SqlTreeNode(
                        id,
                        this.id,
                        level + 1,
                        node,
                        sqlNodeSetter,
                        kindKeyPath)
        );
    }

    private List<SqlKindKey> getKindKeyPath(SqlNode node) {
        val kindPathList = new ArrayList<SqlKindKey>(this.kindPath.size() + 1);
        kindPathList.addAll(this.kindPath);

        if (kindPathList.size() > 0) {
            int lastIndex = kindPathList.size() - 1;
            SqlKindKey lastKindKey = kindPathList.get(lastIndex);
            kindPathList.set(lastIndex, new SqlKindKey(lastKindKey.getSqlKind(), this.childPos > 0 ? this.childPos : null));
        }

        kindPathList.add(new SqlKindKey(node.getKind(), null));

        return kindPathList;
    }

    public void resetChildPos() {
        childPos = 0;
    }

    @SuppressWarnings("unchecked")
    public <T extends SqlNode> T getNode() {
        return (T) node;
    }

    public Optional<String> tryGetSchemaName() {
        if (node instanceof SqlSnapshot) {
            SqlSnapshot snapshot = getNode();
            return tryGetSchemaName(snapshot.getTableRef());
        } else {
            return tryGetSchemaName(node);
        }
    }

    private Optional<String> tryGetSchemaName(SqlNode node) {
        if (node instanceof SqlIdentifier) {
            SqlIdentifier idNode = (SqlIdentifier) node;
            if (idNode.isSimple()) {
                return Optional.empty();
            } else {
                return Optional.of(idNode.names.get(0));
            }
        } else {
            return Optional.empty();
        }
    }

    public Optional<String> tryGetTableName() {
        if (node instanceof SqlSnapshot) {
            SqlSnapshot snapshot = getNode();
            return tryGetTableName(snapshot.getTableRef());
        } else {
            return tryGetTableName(node);
        }
    }

    private Optional<String> tryGetTableName(SqlNode node) {
        if (node instanceof SqlIdentifier) {
            SqlIdentifier idNode = (SqlIdentifier) node;
            if (idNode.isSimple()) {
                return Optional.of(idNode.names.get(0));
            } else {
                return Optional.of(idNode.names.get(1));
            }
        } else {
            return Optional.empty();
        }
    }

    @Override
    public int compareTo(SqlTreeNode o) {
        return Integer.compare(this.getId(), o.getId());
    }
}
