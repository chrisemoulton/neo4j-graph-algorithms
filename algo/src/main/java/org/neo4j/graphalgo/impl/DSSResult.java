package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.predicates.IntIntPredicate;
import com.carrotsearch.hppc.predicates.LongLongPredicate;
import org.neo4j.graphalgo.api.HugeNodeIterator;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.NodeIterator;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;

import java.util.stream.Stream;

public final class DSSResult {
    public final DisjointSetStruct struct;

    public DSSResult(final DisjointSetStruct struct) {
        assert (struct != null);
        this.struct = struct;
    }

    public int getSetCount() {
        return struct.getSetCount();
    }

    public Stream<DisjointSetStruct.Result> resultStream(IdMapping idMapping) {
        return struct.resultStream(idMapping);
    }

    public void forEach(NodeIterator nodes, IntIntPredicate consumer) {
        nodes.forEachNode(nodeId -> consumer.apply(nodeId, struct.find(nodeId)));
    }

    public void forEach(HugeNodeIterator nodes, LongLongPredicate consumer) {
        nodes.forEachNode(nodeId -> consumer.apply((int) nodeId, struct.find((int) nodeId)));
    }
}
