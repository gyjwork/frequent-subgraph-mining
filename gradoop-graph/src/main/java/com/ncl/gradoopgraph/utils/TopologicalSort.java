package com.ncl.gradoopgraph.utils;

import org.apache.flink.api.common.functions.MapFunction;
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
 * @title: TopologicalSort
 * @projectName gradoop-graph
 * @description: TODO
 * @date 6/12/234:11 PM
 */
public class TopologicalSort {

    // This method implements topological sorting
    public static DataSet<EPGMVertex> topologicalSort(LogicalGraph graph,ExecutionEnvironment env) throws Exception {

        // Get the dataset of vertices and edges
        DataSet<EPGMVertex> vertices = graph.getVertices();
        DataSet<EPGMEdge> edges = graph.getEdges();

        // Get the Id of all vertices
        DataSet<GradoopId> vertexIds = vertices.map(new MapFunction<EPGMVertex, GradoopId>() {
            @Override
            public GradoopId map(EPGMVertex vertex) throws Exception {
                return vertex.getId();
            }
        });

        // Create a HashMap to store the degree of entry of each vertex
        HashMap<GradoopId, Integer> inDegreeMap = new HashMap<>();
        for (EPGMEdge edge : edges.collect()) {
            GradoopId targetId = edge.getTargetId();
            inDegreeMap.put(targetId, inDegreeMap.getOrDefault(targetId, 0) + 1);
        }

        // Create a queue to store all vertices with an entry of 0
        Queue<EPGMVertex> queue = new LinkedList<>();
        for (EPGMVertex vertex : vertices.collect()) {
            if (!inDegreeMap.containsKey(vertex.getId())) {
                queue.add(vertex);
            }
        }

        // The result list is used to store the topologically sorted vertices
        List<EPGMVertex> result = new ArrayList<>();
        while (!queue.isEmpty()) {
            EPGMVertex vertex = queue.poll();
            result.add(vertex);

            // Update the degree of entry of adjacent vertices
            for (EPGMEdge edge : edges.collect()) {
                if (edge.getSourceId().equals(vertex.getId())) {
                    GradoopId targetId = edge.getTargetId();
                    int inDegree = inDegreeMap.get(targetId) - 1;
                    if (inDegree == 0) {
                        queue.add(getVertexById(vertices, targetId));
                        inDegreeMap.remove(targetId);
                    } else {
                        inDegreeMap.put(targetId, inDegree);
                    }
                }
            }
        }

        // If the number of vertices in the result list is not equal to the number of vertices in the graph, then there is a ring in the graph, not a DAG
        if (result.size() != vertexIds.count()) {
            throw new Exception("The graph is not a DAG");
        }

        return env.fromCollection(result);
    }

    // Auxiliary method for getting the vertices with a given ID from the dataset
    private static EPGMVertex getVertexById(DataSet<EPGMVertex> vertices, GradoopId id) throws Exception {
        for (EPGMVertex vertex : vertices.collect()) {
            if (vertex.getId().equals(id)) {
                return vertex;
            }
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
//
//        String inputPath = new ClassPathResource("timeSeriesGraph-20230427").getFile().getAbsolutePath();
//        CSVDataSource dataSource = new CSVDataSource(inputPath, config);
//
//        LogicalGraph inputGraph = dataSource.getLogicalGraph();
//        LogicalGraph graph = GraphTransformer.transformGraph(env, inputGraph);
//
//        graph.print();
//
//        DataSet<EPGMVertex> topologicalSort = TopologicalSort.topologicalSort(graph,env);
//        topologicalSort.print();

    }


}
