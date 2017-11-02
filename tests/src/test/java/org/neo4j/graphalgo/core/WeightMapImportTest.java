package org.neo4j.graphalgo.core;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;

/**
 * @author mknblch
 */
public class WeightMapImportTest {

    @Rule
    public ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @Rule
    public ErrorCollector collector = new ErrorCollector();

    private HeavyGraph graph;

    @Test
    public void testWeightsOfInterconnectedNodesWithOutgoing() {
        setup("CREATE (a:N),(b:N) CREATE (a)-[:R{w:1}]->(b),(b)-[:R{w:2}]->(a)", Direction.OUTGOING);

        checkWeight(0, Direction.OUTGOING, 1.0);
        checkWeight(1, Direction.OUTGOING, 2.0);
    }

    @Test
    public void testWeightsOfTriangledNodesWithOutgoing() {
        setup("CREATE (a:N),(b:N),(c:N) CREATE (a)-[:R{w:1}]->(b),(b)-[:R{w:2}]->(c),(c)-[:R{w:3}]->(a)", Direction.OUTGOING);

        checkWeight(0, Direction.OUTGOING, 1.0);
        checkWeight(1, Direction.OUTGOING, 2.0);
        checkWeight(2, Direction.OUTGOING, 3.0);
    }

    @Test
    public void testWeightsOfInterconnectedNodesWithIncoming() {
        setup("CREATE (a:N),(b:N) CREATE (a)-[:R{w:1}]->(b),(b)-[:R{w:2}]->(a)", Direction.INCOMING);

        checkWeight(0, Direction.INCOMING, 2.0);
        checkWeight(1, Direction.INCOMING, 1.0);
    }

    @Test
    public void testWeightsOfTriangledNodesWithIncoming() {
        setup("CREATE (a:N),(b:N),(c:N) CREATE (a)-[:R{w:1}]->(b),(b)-[:R{w:2}]->(c),(c)-[:R{w:3}]->(a)", Direction.INCOMING);

        checkWeight(0, Direction.INCOMING, 3.0);
        checkWeight(1, Direction.INCOMING, 1.0);
        checkWeight(2, Direction.INCOMING, 2.0);
    }

    @Test
    public void testWeightsOfInterconnectedNodesWithBoth() {
        setup("CREATE (a:N),(b:N) CREATE (a)-[:R{w:1}]->(b),(b)-[:R{w:2}]->(a)", Direction.BOTH);

        // loading both overwrites the weights in the following order,
        // which is specific to HeavyGraphFactory
        // a->b: 1  |  a<-b: 2  |  b->a: 2  |  b<-a: 1
        // therefore the final weight for in/outs of either a/b is 1,
        // the weight of 2 is discarded

        checkWeight(0, Direction.OUTGOING, 1.0);
        checkWeight(1, Direction.OUTGOING, 1.0);

        checkWeight(0, Direction.INCOMING, 1.0);
        checkWeight(1, Direction.INCOMING, 1.0);

        checkWeight(0, Direction.BOTH, 1.0, 1.0);
        checkWeight(1, Direction.BOTH, 1.0, 1.0);
    }

    @Test
    public void testWeightsOfTriangledNodesWithBoth() {
        setup("CREATE (a:N),(b:N),(c:N) CREATE (a)-[:R{w:1}]->(b),(b)-[:R{w:2}]->(c),(c)-[:R{w:3}]->(a)", Direction.BOTH);

        checkWeight(0, Direction.OUTGOING, 1.0);
        checkWeight(1, Direction.OUTGOING, 2.0);
        checkWeight(2, Direction.OUTGOING, 3.0);

        checkWeight(0, Direction.INCOMING, 3.0);
        checkWeight(1, Direction.INCOMING, 1.0);
        checkWeight(2, Direction.INCOMING, 2.0);

        checkWeight(0, Direction.BOTH, 3.0, 1.0);
        checkWeight(1, Direction.BOTH, 1.0, 2.0);
        checkWeight(2, Direction.BOTH, 2.0, 3.0);
    }

    private void setup(String cypher, Direction direction) {
        DB.execute(cypher);
        graph = (HeavyGraph) new GraphLoader(DB)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .withDirection(direction)
                .withRelationshipWeightsFromProperty("w", 0.0)
                .load(HeavyGraphFactory.class);
    }

    private void checkWeight(int nodeId, Direction direction, double... expecteds) {
        graph.forEachRelationship(nodeId, direction, checks(direction, expecteds));
    }

    private WeightedRelationshipConsumer checks(Direction direction, double... expecteds) {
        AtomicInteger i = new AtomicInteger();
        int limit = expecteds.length;
        return (s, t, r, w) -> {
            String rel = String.format("(%d %s %d)", s, arrow(direction), t);
            if (i.get() >= limit) {
                collector.addError(new RuntimeException(String.format("Unexpected relationship: %s = %.1f", rel, w)));
                return false;
            }
            double actual = graph.weightOf(s, t);
            double expected = expecteds[i.getAndIncrement()];
            collector.checkThat(String.format("%s (RW): %.1f != %.1f", rel, actual, expected), actual, is(closeTo(expected, 1e-4)));
            collector.checkThat(String.format("%s (WRI): %.1f != %.1f", rel, w, expected), w, is(closeTo(expected, 1e-4)));
            return true;
        };
    }

    private static String arrow(Direction direction) {
        switch (direction) {
            case OUTGOING:
                return "->";
            case INCOMING:
                return "<-";
            case BOTH:
                return "<->";
            default:
                return "???";
        }
    }
}
