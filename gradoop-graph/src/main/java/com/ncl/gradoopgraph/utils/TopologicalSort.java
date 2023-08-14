package com.ncl.gradoopgraph.utils;

import com.ncl.gradoopgraph.loadData.TestData;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import java.util.*;

/**
 * @author gyj
 * @title: TopologicalSort
 * @projectName gradoop-graph
 * @description: TODO
 * @date 6/12/23 4:11 PM
 */
public class TopologicalSort {

    public static List<EPGMVertex> topologicalSort(LogicalGraph graph) throws Exception {
        DataSet<EPGMVertex> vertices = graph.getVertices();
        DataSet<EPGMEdge> edges = graph.getEdges();

        List<EPGMVertex> vertexList = vertices.collect();
        List<EPGMEdge> edgeList = edges.collect();

        HashMap<GradoopId, Integer> inDegreeMap = new HashMap<>();
        for (EPGMEdge edge : edgeList) {
            GradoopId targetId = edge.getTargetId();
            inDegreeMap.put(targetId, inDegreeMap.getOrDefault(targetId, 0) + 1);
        }

        Queue<EPGMVertex> queue = new LinkedList<>();
        for (EPGMVertex vertex : vertexList) {
            if (!inDegreeMap.containsKey(vertex.getId())) {
                queue.add(vertex);
            }
        }

        List<EPGMVertex> result = new ArrayList<>();
        while (!queue.isEmpty()) {
            EPGMVertex vertex = queue.poll();
            result.add(vertex);

            for (EPGMEdge edge : edgeList) {
                if (edge.getSourceId().equals(vertex.getId())) {
                    GradoopId targetId = edge.getTargetId();
                    int inDegree = inDegreeMap.get(targetId) - 1;
                    if (inDegree == 0) {
                        queue.add(getVertexById(vertexList, targetId));
                        inDegreeMap.remove(targetId);
                    } else {
                        inDegreeMap.put(targetId, inDegree);
                    }
                }
            }
        }

        if (result.size() != vertexList.size()) {
            throw new Exception("The graph is not a DAG");
        }

        return result;
    }

    private static EPGMVertex getVertexById(List<EPGMVertex> vertices, GradoopId id) {
        for (EPGMVertex vertex : vertices) {
            if (vertex.getId().equals(id)) {
                return vertex;
            }
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        LogicalGraph graph = TestData.loadTestData(env);
        graph.print();

        List<EPGMVertex> topologicalSort = TopologicalSort.topologicalSort(graph);
        topologicalSort.stream().forEach(System.out::println);

    }


}
