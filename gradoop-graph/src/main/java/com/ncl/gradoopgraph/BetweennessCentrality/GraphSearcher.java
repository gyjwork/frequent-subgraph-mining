package com.ncl.gradoopgraph.BetweennessCentrality;

import com.ncl.gradoopgraph.utils.GraphTransformer;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.Collector;
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
 * @title: GraphSearcher
 * @projectName gradoop-graph
 * @description: TODO
 * @date 5/27/2310:55 PM
 */


public class GraphSearcher {

    /**
     * This method filters the edges based on the SearchType provided as input and collects the IDs of neighboring vertices
     * @param graph
     * @param vertexId
     * @param type
     * @return
     * @throws Exception
     */
    public DataSet<EPGMVertex> getNeighbors(LogicalGraph graph, GradoopId vertexId, SearchType type) throws Exception {
        DataSet<EPGMEdge> edges = graph.getEdges().filter(new FilterFunction<EPGMEdge>() {
            @Override
            public boolean filter(EPGMEdge edge) {
                // Depending on the search type, we filter the edges
                switch (type) {
                    case OUTGOING:
                        return edge.getSourceId().equals(vertexId);
                    case INCOMING:
                        return edge.getTargetId().equals(vertexId);
                    case UNDIRECTED:
                        return edge.getSourceId().equals(vertexId) || edge.getTargetId().equals(vertexId);
                    default:
                        throw new UnsupportedOperationException("Unsupported search type: " + type);
                }
            }
        });

        // Mapping the edges to the corresponding neighbor vertex IDs
        DataSet<GradoopId> neighborIds = edges.flatMap(new FlatMapFunction<EPGMEdge, GradoopId>() {
            @Override
            public void flatMap(EPGMEdge edge, Collector<GradoopId> out) {
                if (edge.getSourceId().equals(vertexId)) {
                    out.collect(edge.getTargetId());
                } else {
                    out.collect(edge.getSourceId());
                }
            }
        });

        // Joining with the vertices dataset to get the vertex data
        DataSet<EPGMVertex> neighbors = graph.getVertices().join(neighborIds)
                .where(new KeySelector<EPGMVertex, GradoopId>() {
                    @Override
                    public GradoopId getKey(EPGMVertex vertex) {
                        return vertex.getId();
                    }
                })
                .equalTo(new KeySelector<GradoopId, GradoopId>() {
                    @Override
                    public GradoopId getKey(GradoopId gradoopId) {
                        return gradoopId;
                    }
                })
                .with(new JoinFunction<EPGMVertex, GradoopId, EPGMVertex>() {
                    @Override
                    public EPGMVertex join(EPGMVertex vertex, GradoopId gradoopId) {
                        return vertex;
                    }
                });

        return neighbors;
    }

    /**
     * This method implements the DFS algorithm
     * @param graph
     * @param vertexId
     * @param visited
     * @param path
     * @param type
     * @throws Exception
     */
    public void dfs(LogicalGraph graph, GradoopId vertexId, Set<GradoopId> visited, LinkedList<GradoopId> path, SearchType type) throws Exception {
        visited.add(vertexId);
        path.add(vertexId);
        System.out.println("Current path: " + path);

        DataSet<EPGMVertex> neighbors = getNeighbors(graph, vertexId, type);

        // Iterating over all the neighbors of the current vertex
        for (EPGMVertex neighbor : neighbors.collect()) {
            GradoopId neighborId = neighbor.getId();
            // If the neighbor has not been visited, we perform a DFS on that vertex
            if (!visited.contains(neighborId)) {
                dfs(graph, neighborId, visited, path, type);
            }
        }

        path.remove(vertexId);
    }

    /**
     * This method implements the BFS algorithm
     * @param graph
     * @param startVertexId
     * @param type
     * @throws Exception
     */
    public void bfs(LogicalGraph graph, GradoopId startVertexId, SearchType type) throws Exception {
        Set<GradoopId> visited = new HashSet<>();
        Queue<GradoopId> queue = new LinkedList<>();

        Map<GradoopId, LinkedList<GradoopId>> paths = new HashMap<>();
        LinkedList<GradoopId> startPath = new LinkedList<>();
        startPath.add(startVertexId);
        paths.put(startVertexId, startPath);

        visited.add(startVertexId);
        queue.add(startVertexId);

        // While there are vertices in the queue
        while (!queue.isEmpty()) {
            GradoopId currentVertexId = queue.poll();

            LinkedList<GradoopId> currentPath = paths.get(currentVertexId);
            System.out.println("Current path: " + currentPath);

            DataSet<EPGMVertex> neighbors = getNeighbors(graph, currentVertexId, type);

            // Iterating over all the neighbors of the current vertex
            for (EPGMVertex neighbor : neighbors.collect()) {
                GradoopId neighborId = neighbor.getId();
                // If the neighbor has not been visited, we add it to the queue and the visited set
                if (!visited.contains(neighborId)) {
                    visited.add(neighborId);
                    queue.add(neighborId);

                    LinkedList<GradoopId> newPath = new LinkedList<>(currentPath);
                    newPath.add(neighborId);
                    paths.put(neighborId, newPath);
                }
            }
        }
    }

    public void searchDFS(LogicalGraph graph, GradoopId startVertexId, SearchType type) throws Exception {
        Set<GradoopId> visited = new HashSet<>();
        LinkedList<GradoopId> path = new LinkedList<>();
        dfs(graph, startVertexId, visited, path, type);
    }

    public void searchBFS(LogicalGraph graph, GradoopId startVertexId, SearchType type) throws Exception {
        bfs(graph, startVertexId, type);
    }

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

        String inputPath = new ClassPathResource("timeSeriesGraph-20230427").getFile().getAbsolutePath();
        CSVDataSource dataSource = new CSVDataSource(inputPath, config);

        LogicalGraph inputGraph = dataSource.getLogicalGraph();
        LogicalGraph graph = GraphTransformer.transformGraph(env, inputGraph);

        BetweennessCalculator calculator = new BetweennessCalculator(graph);
        Map<GradoopId, Double> scores = calculator.calculateWithSmallGraph();

        List<Map.Entry<GradoopId, Double>> sortedScores = new ArrayList<>(scores.entrySet());

        sortedScores.sort((entry1, entry2) -> entry2.getValue().compareTo(entry1.getValue()));

        Map.Entry<GradoopId, Double> highestScoreEntry = sortedScores.get(0);
        System.out.println("Vertex with the highest Betweenness Centrality: ID = " + highestScoreEntry.getKey() + ", Score = " + highestScoreEntry.getValue());

        GraphSearcher searcher = new GraphSearcher();
        GradoopId startVertexId = highestScoreEntry.getKey();
        // For DFS search
        System.out.println("DFS - OUTGOING");
        searcher.searchDFS(graph, startVertexId, SearchType.OUTGOING);
        System.out.println("DFS - INCOMING");
        searcher.searchDFS(graph, startVertexId, SearchType.INCOMING);
        System.out.println("DFS - UNDIRECTED");
        searcher.searchDFS(graph, startVertexId, SearchType.UNDIRECTED);

        // For BFS search
        System.out.println("BFS - OUTGOING");
        searcher.searchBFS(graph, startVertexId, SearchType.OUTGOING);
        System.out.println("BFS - INCOMING");
        searcher.searchBFS(graph, startVertexId, SearchType.INCOMING);
        System.out.println("BFS - UNDIRECTED");
        searcher.searchBFS(graph, startVertexId, SearchType.UNDIRECTED);
    }
}



