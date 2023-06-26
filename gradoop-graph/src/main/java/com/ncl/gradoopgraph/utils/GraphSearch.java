package com.ncl.gradoopgraph.utils;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.springframework.core.io.ClassPathResource;

import java.util.*;

/**
 * @author gyj
 * @title: GraphSearch
 * @projectName gradoop-graph
 * @description: Use TopologicalSort to search all sinple paths
 * @date 6/26/235:31 PM
 */
public class GraphSearch {
    // Auxiliary method for getting the vertices with a given ID from the dataset
    private static EPGMVertex getVertexById(Collection<EPGMVertex> vertices, GradoopId id) throws Exception {
        for (EPGMVertex vertex : vertices) {
            if (vertex.getId().equals(id)) {
                return vertex;
            }
        }
        return null;
    }

    // Recursive DFS
// Recursive DFS
    private static void dfs(GradoopId currentId, List<GradoopId> visited, Map<GradoopId, List<GradoopId>> adjList) {
        visited.add(currentId);
        List<GradoopId> neighbors = adjList.get(currentId);
        if (neighbors != null) {
            for (GradoopId id : neighbors) {
                if (!visited.contains(id)) {
                    dfs(id, visited, adjList);
                }
            }
        }
    }

    // Print all paths
    public static void printAllPaths(LogicalGraph graph, DataSet<EPGMVertex> sortedVertices) throws Exception {
        Collection<EPGMVertex> vertices = sortedVertices.collect();
        Map<GradoopId, List<GradoopId>> adjList = new HashMap<>();

        for (EPGMEdge edge : graph.getEdges().collect()) {
            GradoopId sourceId = edge.getSourceId();
            GradoopId targetId = edge.getTargetId();
            if (!adjList.containsKey(sourceId)) {
                adjList.put(sourceId, new ArrayList<>());
            }
            adjList.get(sourceId).add(targetId);
        }

        for (EPGMVertex vertex : vertices) {
            List<GradoopId> visited = new ArrayList<>();
            dfs(vertex.getId(), visited, adjList);
            System.out.println("Path starting from vertex " + vertex.getId() + ":");
            for (GradoopId id : visited) {
                System.out.println(getVertexById(vertices, id));
            }
            System.out.println();
        }
    }

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

        String inputPath = new ClassPathResource("timeSeriesGraph-20230427").getFile().getAbsolutePath();
        CSVDataSource dataSource = new CSVDataSource(inputPath, config);

        LogicalGraph inputGraph = dataSource.getLogicalGraph();
        LogicalGraph graph = GraphTransformer.transformGraph(env, inputGraph);

        graph.print();

        DataSet<EPGMVertex> topologicalSort = TopologicalSort.topologicalSort(graph, env);
        topologicalSort.print();

        printAllPaths(graph, topologicalSort);

    }
}
