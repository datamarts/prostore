package ru.datamart.prostore.query.calcite.core.node;

import ru.datamart.prostore.query.calcite.core.extension.dml.LimitableSqlOrderBy;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.apache.calcite.sql.*;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.util.*;
import java.util.stream.Collectors;

@Data
@Slf4j
public class SqlSelectTree {
    public static final SqlPredicates IS_TABLE_OR_SNAPSHOTS = SqlPredicates.builder()
            .anyOf(SqlPredicatePart.anyFromStart(), SqlPredicatePart.eqWithNum(SqlKind.JOIN), SqlPredicatePart.eq(SqlKind.SELECT))
            .maybeOf(SqlPredicatePart.eq(SqlKind.AS))
            .anyOf(SqlPredicatePart.eq(SqlKind.SNAPSHOT), SqlPredicatePart.eq(SqlKind.IDENTIFIER))
            .build();
    private static final SqlPredicates IS_TABLE_OR_SNAPSHOTS_WITH_CHILD = SqlPredicates.builder()
            .anyOf(SqlPredicatePart.anyFromStart(), SqlPredicatePart.eqWithNum(SqlKind.JOIN), SqlPredicatePart.eq(SqlKind.SELECT))
            .maybeOf(SqlPredicatePart.eq(SqlKind.AS))
            .anyOf(SqlPredicatePart.eq(SqlKind.SNAPSHOT), SqlPredicatePart.eq(SqlKind.IDENTIFIER))
            .maybeOf(SqlPredicatePart.eq(SqlKind.IDENTIFIER))
            .build();
    private static final SqlPredicates SELECT_AS_SNAPSHOT = SqlPredicates.builder()
            .anyOf(SqlPredicatePart.eq(SqlKind.SNAPSHOT))
            .build();

    private static final String QUERY_FIELD = "query";
    private static final String COLUMN_LIST_FIELD = "columnList";
    private static final String NAME_FIELD = "name";
    private final Map<Integer, SqlTreeNode> nodeMap;
    private final Map<Integer, List<SqlTreeNode>> childNodesMap;
    private final Map<SqlKind, List<SqlTreeNode>> lastKindToNode;
    private int idCounter;

    public SqlSelectTree(SqlNode sqlSelect) {
        nodeMap = new HashMap<>();
        childNodesMap = new HashMap<>();
        lastKindToNode = new TreeMap<>();
        createRoot(sqlSelect).ifPresent(this::addNodes);
    }

    SqlSelectTree(Map<Integer, SqlTreeNode> nodeMap, Map<Integer, List<SqlTreeNode>> childNodesMap, Map<SqlKind, List<SqlTreeNode>> lastKindToNode, int idCounter) {
        this.idCounter = idCounter;
        this.nodeMap = nodeMap;
        this.childNodesMap = childNodesMap;
        this.lastKindToNode = lastKindToNode;
    }

    public SqlTreeNode getRoot() {
        return nodeMap.get(0);
    }

    public Optional<SqlTreeNode> getParentByChild(SqlTreeNode child) {
        return getParentByChild(nodeMap, child);
    }

    private Optional<SqlTreeNode> getParentByChild(Map<Integer, SqlTreeNode> nodeMap, SqlTreeNode child) {
        if (child == null) {
            return Optional.empty();
        }

        return Optional.ofNullable(nodeMap.get(child.getParentId()));
    }

    public List<SqlTreeNode> findSnapshots() {
        return findNodes(SELECT_AS_SNAPSHOT, true);
    }

    public List<SqlTreeNode> findAllTableAndSnapshots() {
        return findNodes(IS_TABLE_OR_SNAPSHOTS, true);
    }

    public List<SqlTreeNode> findAllTableAndSnapshotWithChildren() {
        return findNodes(IS_TABLE_OR_SNAPSHOTS_WITH_CHILD, false);
    }

    public List<SqlTreeNode> findNodes(SqlPredicates predicates, boolean filterChildren) {
        if (predicates == null || predicates.isEmpty()) {
            return new ArrayList<>(nodeMap.values());
        }

        var initialNodes = gatherInitialNodesByPredicate(predicates);
        if (predicates.size() == 1) {
            if (filterChildren) {
                initialNodes = filterChild(initialNodes);
            }
            initialNodes.sort(SqlTreeNode::compareTo);
            return initialNodes;
        }

        List<SqlTreeNode> result = new ArrayList<SqlTreeNode>(initialNodes.size());
        for (SqlTreeNode node : initialNodes) {
            val kindPath = node.getKindPath();
            if (kindPath.size() < predicates.getNotMaybePredicates()) {
                continue;
            }

            int currentPredicateIndex = predicates.size() - 1;
            int currentKindIndex = kindPath.size() - 1;
            if (checkPredicate(predicates, kindPath, currentPredicateIndex, currentKindIndex)) {
                result.add(node);
            }
        }

        if (filterChildren) {
            result = filterChild(result);
        }

        result.sort(SqlTreeNode::compareTo);

        return result;
    }

    private boolean checkPredicate(SqlPredicates predicates, List<SqlKindKey> kindPath, int currentPredicateIndex, int currentKindIndex) {
        if (currentKindIndex < 0 || currentPredicateIndex < 0) {
            return currentPredicateIndex < 0;
        }

        val predicate = predicates.get(currentPredicateIndex);
        val nodeKind = kindPath.get(currentKindIndex);
        boolean passed = predicate.test(currentKindIndex, nodeKind);
        if (!passed) {
            if (predicate.getType() != SqlPredicateType.MAYBE) {
                return false;
            }

            return checkPredicate(predicates, kindPath, currentPredicateIndex - 1, currentKindIndex);
        }

        if (predicate.getType() == SqlPredicateType.MAYBE) {
            // this is needed to check both branches from "maybe" predicate on pass (maybe can be skipped - cause next predicate can pass instead of "maybe" one)
            return checkPredicate(predicates, kindPath, currentPredicateIndex - 1, currentKindIndex - 1) ||
                    checkPredicate(predicates, kindPath, currentPredicateIndex - 1, currentKindIndex);
        }

        return checkPredicate(predicates, kindPath, currentPredicateIndex - 1, currentKindIndex - 1);
    }

    public List<SqlTreeNode> findNodesByParent(SqlTreeNode sqlTreeNode) {
        return childNodesMap.getOrDefault(sqlTreeNode.getId(), Collections.emptyList());
    }

    private List<SqlTreeNode> filterChild(List<SqlTreeNode> nodeList) {
        return nodeList.stream()
                .filter(n1 -> nodeList.stream().noneMatch(n2 -> n1.getParentId() == n2.getId()))
                .sorted()
                .collect(Collectors.toList());
    }

    private List<SqlTreeNode> gatherInitialNodesByPredicate(SqlPredicates sqlPredicate) {
        List<SqlTreeNode> result = new ArrayList<>();
        for (SqlKind type : sqlPredicate.getInitialKinds()) {
            result.addAll(lastKindToNode.getOrDefault(type, Collections.emptyList()));
        }
        return result;
    }

    private void flattenSql(SqlTreeNode treeNode) {
        val node = treeNode.getNode();
        if (node instanceof SqlSelect) {
            flattenSqlSelect(treeNode, (SqlSelect) node);
        } else if (node instanceof SqlNodeList) {
            flattenSqlNodeList(treeNode, (SqlNodeList) node);
        } else if (node instanceof SqlJoin) {
            flattenSqlJoin(treeNode, (SqlJoin) node);
        } else if (node instanceof SqlIdentifier) {
            flattenSqlIdentifier(treeNode);
        } else if (node instanceof LimitableSqlOrderBy) {
            flattenLimitableSqlOrderBy(treeNode, (LimitableSqlOrderBy) node);
        } else if (node instanceof SqlSnapshot) {
            flattenSqlSnapshot(treeNode, (SqlSnapshot) node);
        } else if (node instanceof SqlBasicCall) {
            flattenSqlBasicCall(treeNode, (SqlBasicCall) node);
        } else if (node instanceof SqlInsert) {
            flattenSqlInsert(treeNode, (SqlCall) node);
        } else if (node instanceof SqlCreate) {
            flattenSqlCreate(treeNode, (SqlDdl) node);
        } else if (node instanceof SqlDrop) {
            flattenSqlDrop(treeNode, (SqlDdl) node);
        } else if (node instanceof SqlAlter) {
            flattenSqlAlterView(treeNode, (SqlCall) node);
        } else if (node instanceof SqlCall) {
            flattenSqlCall(treeNode, (SqlCall) node);
        }
    }

    private void flattenLimitableSqlOrderBy(SqlTreeNode treeNode, LimitableSqlOrderBy node) {
        flattenSqlCall(treeNode, node);
    }

    private void flattenSqlAlterView(SqlTreeNode parentTree, SqlCall parentNode) {
        addReflectNode(parentTree, parentNode, NAME_FIELD);
        parentTree.resetChildPos();
        addReflectNode(parentTree, parentNode, COLUMN_LIST_FIELD);
        parentTree.resetChildPos();
        addReflectNode(parentTree, parentNode, QUERY_FIELD);
    }

    private void flattenSqlCreate(SqlTreeNode parentTree, SqlDdl parentNode) {
        flattenSqlDrop(parentTree, parentNode);
        parentTree.resetChildPos();
        addReflectNode(parentTree, parentNode, COLUMN_LIST_FIELD);
        parentTree.resetChildPos();
        addReflectNode(parentTree, parentNode, QUERY_FIELD);
    }

    private void addReflectNode(SqlTreeNode parentTree, SqlNode parentNode, String fieldName) {
        parentTree.createChild(getNextId(),
                        readNode(parentNode, fieldName),
                        new SqlNodeConsumer<>(parentNode, (parent, child) -> writeNode(parent, child, fieldName)))
                .ifPresent(this::addNodes);
    }

    private void flattenSqlDrop(SqlTreeNode parentTree, SqlDdl parentNode) {
        addReflectNode(parentTree, parentNode, NAME_FIELD);
    }

    @SneakyThrows
    private SqlNode readNode(SqlNode o, String fieldName) {
        try {
            return (SqlNode) FieldUtils.readField(o, fieldName, true);
        } catch (Exception ex) {
            return null;
        }
    }

    @SneakyThrows
    private void writeNode(SqlNode parent, SqlNode child, String fieldName) {
        FieldUtils.writeField(parent, fieldName, child, true);
    }

    private void flattenSqlInsert(SqlTreeNode parentTree, SqlCall parentNode) {
        val nodes = parentNode.getOperandList();
        for (int i = 0; i < nodes.size(); i++) {
            val itemNode = nodes.get(i);
            int finalI = i;
            parentTree.resetChildPos();
            parentTree.createChild(getNextId(),
                            itemNode,
                            new SqlNodeConsumer<>(parentNode, (sqlCall, sqlNode) -> sqlCall.setOperand(finalI, sqlNode)))
                    .ifPresent(this::addNodes);
        }
    }

    private void flattenSqlBasicCall(SqlTreeNode parentTree, SqlBasicCall parentNode) {
        flattenSqlCall(parentTree, parentNode);
    }

    private void flattenSqlSnapshot(SqlTreeNode parent, SqlSnapshot parentNode) {
        flattenSqlCall(parent, parentNode);
    }

    private void flattenSqlIdentifier(SqlTreeNode parentTree) {
        parentTree.setIdentifier(true);
    }

    private void flattenSqlCall(SqlTreeNode parentTree, SqlCall parentNode) {
        val nodes = parentNode.getOperandList();
        for (int i = 0; i < nodes.size(); i++) {
            val itemNode = nodes.get(i);
            int finalI = i;
            parentTree.createChild(getNextId(),
                            itemNode,
                            new SqlNodeConsumer<>(parentNode, (sqlCall, sqlNode) -> sqlCall.setOperand(finalI, sqlNode)))
                    .ifPresent(this::addNodes);
        }
    }

    private void flattenSqlSelect(SqlTreeNode parentTree, SqlSelect parentNode) {
        parentTree.createChild(getNextId(),
                        parentNode.getSelectList(),
                        new SqlNodeConsumer<>(parentNode, (sqlSelect, sqlNode) -> sqlSelect.setSelectList((SqlNodeList) sqlNode)))
                .ifPresent(this::addNodes);
        parentTree.resetChildPos();
        parentTree.createChild(getNextId(), parentNode.getWhere(), new SqlNodeConsumer<>(parentNode, SqlSelect::setWhere))
                .ifPresent(this::addNodes);
        parentTree.resetChildPos();
        parentTree.createChild(getNextId(),
                        parentNode.getFrom(),
                        new SqlNodeConsumer<>(parentNode, SqlSelect::setFrom))
                .ifPresent(this::addNodes);
        parentTree.resetChildPos();
        parentTree.createChild(getNextId(),
                        parentNode.getFetch(),
                        new SqlNodeConsumer<>(parentNode, SqlSelect::setFetch))
                .ifPresent(this::addNodes);
        parentTree.resetChildPos();
        parentTree.createChild(getNextId(),
                        parentNode.getOffset(),
                        new SqlNodeConsumer<>(parentNode, SqlSelect::setOffset))
                .ifPresent(this::addNodes);
    }

    private void flattenSqlJoin(SqlTreeNode parentTree, SqlJoin parentNode) {
        flattenSqlCall(parentTree, parentNode);
    }

    private void flattenSqlNodeList(SqlTreeNode parentTree, SqlNodeList parentNode) {
        val nodes = parentNode.getList();
        for (int i = 0; i < nodes.size(); i++) {
            val itemNode = nodes.get(i);
            int finalI = i;
            parentTree.createChild(getNextId(),
                            itemNode,
                            new SqlNodeConsumer<>(parentNode, (sqlNodes, sqlNode) -> sqlNodes.set(finalI, sqlNode)))
                    .ifPresent(this::addNodes);
        }
    }

    private void addNodes(SqlTreeNode... nodes) {
        for (val node : nodes) {
            nodeMap.put(node.getId(), node);
            lastKindToNode.computeIfAbsent(node.getNode().getKind(), sqlKind -> new ArrayList<>()).add(node);
            childNodesMap.computeIfAbsent(node.getParentId(), i -> new ArrayList<>()).add(node);
            flattenSql(node);
        }
    }

    private Optional<SqlTreeNode> createRoot(SqlNode node) {
        return Optional.of(new SqlTreeNode(getNextId(),
                -1,
                0,
                node,
                null,
                Arrays.asList(new SqlKindKey(node.getKind(), null))));
    }

    private int getNextId() {
        return idCounter++;
    }

    public SqlSelectTree copy() {
        val copiedSqlNodeMap = nodeMap.entrySet().stream()
                .map(this::getNodePairWithCopiedSqlNode)
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        val resultNodeMap = copiedSqlNodeMap.entrySet().stream()
                .map(e -> getNodePairWithNewNodeSetter(copiedSqlNodeMap, e))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        return new SqlSelectTree(resultNodeMap, childNodesMap, lastKindToNode, idCounter);
    }

    private Pair<Integer, SqlTreeNode> getNodePairWithCopiedSqlNode(Map.Entry<Integer, SqlTreeNode> e) {
        SqlNode origNode = e.getValue().getNode();
        SqlNode clonedNode;
        if (origNode instanceof SqlBasicCall) {
            val sqlBasicCall = (SqlBasicCall) origNode;
            clonedNode = sqlBasicCall.getOperator().createCall(sqlBasicCall.getFunctionQuantifier(),
                    sqlBasicCall.getParserPosition(),
                    Arrays.copyOf(sqlBasicCall.operands, sqlBasicCall.operands.length));
        } else if (origNode instanceof SqlInsert) {
            val insert = (SqlInsert) origNode;
            clonedNode = new SqlInsert(insert.getParserPosition(), (SqlNodeList) insert.getOperandList().get(0), insert.getTargetTable(), insert.getSource(), insert.getTargetColumnList());
        } else if (origNode instanceof SqlDelete) {
            val delete = (SqlDelete) origNode;
            clonedNode = new SqlDelete(delete.getParserPosition(), delete.getTargetTable(), delete.getCondition(), delete.getSourceSelect(), delete.getAlias());
        } else {
            clonedNode = SqlNode.clone(origNode);
        }
        return Pair.of(e.getKey(),
                e.getValue().toBuilder()
                        .node(clonedNode)
                        .build());
    }

    private Pair<Integer, SqlTreeNode> getNodePairWithNewNodeSetter(Map<Integer, SqlTreeNode> sqlNodeMap,
                                                                    Map.Entry<Integer, SqlTreeNode> e) {
        SqlTreeNode origTreeNode = e.getValue();
        val parentOpt = getParentByChild(sqlNodeMap, origTreeNode);
        if (parentOpt.isPresent()) {
            SqlNode parentNode = parentOpt.get().getNode();
            val nwSqlNodeSetter = origTreeNode.getSqlNodeSetter().toBuilder()
                    .parentNode(parentNode)
                    .build();
            val clonedTreeNode = origTreeNode.toBuilder()
                    .sqlNodeSetter(nwSqlNodeSetter)
                    .build();

            clonedTreeNode.getSqlNodeSetter().accept(clonedTreeNode.getNode());
            return Pair.of(e.getKey(), clonedTreeNode);
        } else {
            return Pair.of(e.getKey(), e.getValue());
        }
    }
}
