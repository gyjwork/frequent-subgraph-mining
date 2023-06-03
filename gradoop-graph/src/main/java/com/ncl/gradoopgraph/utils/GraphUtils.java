package com.ncl.gradoopgraph.utils;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

/**
 * @author gyj
 * @title: GraphUtils
 * @projectName gradoop-graph
 * @description: TODO
 * @date 5/19/2310:39 PM
 */
public class GraphUtils {

    public static DataSet<EPGMVertex> getVertexById(LogicalGraph graph, GradoopId id) {
        return graph.getVertices().filter(vertex -> vertex.getId().equals(id));
    }

    public static DataSet<EPGMEdge> getEdgeById(LogicalGraph graph, GradoopId id) {
        return graph.getEdges().filter(edge -> edge.getId().equals(id));
    }
}

