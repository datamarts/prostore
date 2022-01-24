/*
 * Copyright © 2021 ProStore
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
package io.arenadata.dtm.query.execution.core.base.service.delta;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.delta.DeltaInformationResult;
import io.arenadata.dtm.common.delta.DeltaType;
import io.arenadata.dtm.common.delta.SelectOnInterval;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.query.calcite.core.extension.snapshot.SqlDeltaSnapshot;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.calcite.core.node.SqlTreeNode;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Service
public class DeltaInformationExtractor {

    public DeltaInformationResult extract(SqlNode root) {
        try {
            val tree = new SqlSelectTree(root);
            val allTableAndSnapshots = tree.findAllTableAndSnapshots();
            val deltaInformations = getDeltaInformations(tree, allTableAndSnapshots);
            replaceSnapshots(getSnapshots(allTableAndSnapshots));
            return new DeltaInformationResult(deltaInformations, root);
        } catch (Exception e) {
            throw new DtmException("Error extracting delta information", e);
        }
    }

    private List<DeltaInformation> getDeltaInformations(SqlSelectTree tree, List<SqlTreeNode> talbeAndSnapshotList) {
        return talbeAndSnapshotList.stream()
                .map(tableOrSnapshot -> getDeltaInformationAndReplace(tree, tableOrSnapshot))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private List<SqlTreeNode> getSnapshots(List<SqlTreeNode> nodes) {
        return nodes.stream()
                .filter(n -> n.getNode() instanceof SqlDeltaSnapshot)
                .collect(Collectors.toList());
    }

    private void replaceSnapshots(List<SqlTreeNode> snapshots) {
        for (int i = snapshots.size() - 1; i >= 0; i--) {
            val snapshot = snapshots.get(i);
            SqlDeltaSnapshot sqlDeltaSnapshot = snapshot.getNode();
            snapshot.getSqlNodeSetter().accept(sqlDeltaSnapshot.getTableRef());
        }
    }

    public DeltaInformation getDeltaInformation(SqlSelectTree tree, SqlTreeNode tableOrSnapshot) {
        Optional<SqlTreeNode> optParent = tree.getParentByChild(tableOrSnapshot);
        if (optParent.isPresent()) {
            SqlTreeNode parent = optParent.get();
            if (parent.getNode() instanceof SqlBasicCall) {
                return fromSqlBasicCall(parent.getNode(), false);
            }
        }
        return getDeltaInformation(tableOrSnapshot);
    }

    public DeltaInformation getDeltaInformationAndReplace(SqlSelectTree tree, SqlTreeNode tableOrSnapshot) {
        Optional<SqlTreeNode> optParent = tree.getParentByChild(tableOrSnapshot);
        if (optParent.isPresent()) {
            SqlTreeNode parent = optParent.get();
            if (parent.getNode() instanceof SqlBasicCall) {
                return fromSqlBasicCall(parent.getNode(), true);
            }
        }
        return getDeltaInformation(tableOrSnapshot);
    }

    private DeltaInformation getDeltaInformation(SqlTreeNode n) {
        if (n.getNode() instanceof SqlIdentifier) {
            return fromIdentifier(n.getNode(), null, null, false,
                    null, null, null, null);
        }
        if (n.getNode() instanceof SqlBasicCall) {
            return fromIdentifier(n.getNode(), null, null, false,
                    null, null, null, null);
        } else {
            return fromSnapshot(n.getNode(), null);
        }
    }

    private DeltaInformation fromSqlBasicCall(SqlBasicCall basicCall, boolean replace) {
        DeltaInformation deltaInformation = null;
        if (basicCall.getKind() == SqlKind.AS) {
            if (basicCall.operands.length != 2) {
                log.warn("Suspicious AS relation {}", basicCall);
            } else {
                SqlNode left = basicCall.operands[0];
                SqlNode right = basicCall.operands[1];
                if (!(right instanceof SqlIdentifier)) {
                    log.warn("Expecting Sql;Identifier as alias, got {}", right);
                } else if (left instanceof SqlDeltaSnapshot) {
                    if (replace) {
                        SqlIdentifier newId = (SqlIdentifier) ((SqlDeltaSnapshot) left).getTableRef();
                        basicCall.operands[0] = newId;
                    }
                    deltaInformation = fromSnapshot((SqlDeltaSnapshot) left, (SqlIdentifier) right);
                } else if (left instanceof SqlIdentifier) {
                    deltaInformation = fromIdentifier((SqlIdentifier) left, (SqlIdentifier) right, null,
                            false, null, null, null, null);
                }
            }
        }
        return deltaInformation;
    }

    private DeltaInformation fromSnapshot(SqlDeltaSnapshot snapshot, SqlIdentifier alias) {
        return fromIdentifier((SqlIdentifier) snapshot.getTableRef(), alias, snapshot.getDeltaDateTime(),
                snapshot.getLatestUncommittedDelta(), snapshot.getDeltaNum(), snapshot.getStartedInterval(),
                snapshot.getFinishedInterval(), snapshot.getParserPosition());
    }

    private DeltaInformation fromIdentifier(SqlIdentifier id,
                                            SqlIdentifier alias,
                                            String snapshotTime,
                                            boolean isLatestUncommittedDelta,
                                            Long deltaNum,
                                            SelectOnInterval startedIn,
                                            SelectOnInterval finishedIn,
                                            SqlParserPos pos) {
        String datamart = "";
        String tableName;
        if (id.names.size() > 1) {
            datamart = id.names.get(0);
            tableName = id.names.get(1);
        } else {
            tableName = id.names.get(0);
        }

        String aliasVal = "";
        if (alias != null) {
            aliasVal = alias.names.get(0);
        }
        String deltaTime = null;
        DeltaType deltaType = DeltaType.NUM;
        if (!isLatestUncommittedDelta) {
            if (snapshotTime == null) {
                if (deltaNum == null) {
                    deltaType = DeltaType.WITHOUT_SNAPSHOT;
                }
            } else {
                deltaTime = snapshotTime;
                deltaType = DeltaType.DATETIME;
            }
        }

        SelectOnInterval selectOnInterval = null;
        if (startedIn != null) {
            selectOnInterval = startedIn;
            deltaType = DeltaType.STARTED_IN;
        } else if (finishedIn != null) {
            selectOnInterval = finishedIn;
            deltaType = DeltaType.FINISHED_IN;
        }

        return new DeltaInformation(
                aliasVal,
                deltaTime,
                isLatestUncommittedDelta,
                deltaType,
                deltaNum,
                selectOnInterval,
                datamart,
                tableName,
                pos);
    }
}
