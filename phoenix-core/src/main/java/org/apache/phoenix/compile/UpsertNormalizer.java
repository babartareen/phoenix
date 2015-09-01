package org.apache.phoenix.compile;

import com.google.common.collect.Lists;
import org.apache.phoenix.parse.*;

import java.sql.SQLException;
import java.util.List;

public class UpsertNormalizer extends ParseNodeRewriter {

    public static UpsertStatement normalize(UpsertStatement upsert) throws SQLException {
        if (upsert.getCompareNode() == null) {
            return upsert;
        }

        ParseNode compare = rewrite(upsert.getCompareNode(), new UpsertNormalizer());
        return upsert.updateCompare(compare);
    }

    @Override
    public ParseNode visitLeave(final BetweenParseNode node, List<ParseNode> nodes) throws SQLException {
        LessThanOrEqualParseNode lhsNode =  NODE_FACTORY.lte(node.getChildren().get(1), node.getChildren().get(0));
        LessThanOrEqualParseNode rhsNode =  NODE_FACTORY.lte(node.getChildren().get(0), node.getChildren().get(2));
        List<ParseNode> parseNodes = Lists.newArrayListWithExpectedSize(2);
        parseNodes.add(this.visitLeave(lhsNode, lhsNode.getChildren()));
        parseNodes.add(this.visitLeave(rhsNode, rhsNode.getChildren()));
        return super.visitLeave(node, parseNodes);
    }
}
