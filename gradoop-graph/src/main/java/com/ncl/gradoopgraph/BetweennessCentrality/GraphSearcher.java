package com.ncl.gradoopgraph.BetweennessCentrality;

import com.ncl.gradoopgraph.utils.GraphTransformer;
import com.ncl.gradoopgraph.utils.GraphUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
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
     * getNeighbors方法返回给定顶点的邻居节点集合。它首先找到所有与给定顶点相连的边，然后从这些边中提取出邻居节点的ID，最后通过ID获取邻居节点。
     * @param graph
     * @param vertexId
     * @return
     * @throws Exception
     */
    public DataSet<EPGMVertex> getNeighbors(LogicalGraph graph, GradoopId vertexId, boolean forwardSearch) throws Exception {
        // Get the dataset of edges where the source or target matches the provided vertexId
        DataSet<EPGMEdge> edges;
        if (forwardSearch) {
            edges = graph.getEdges().filter(new FilterFunction<EPGMEdge>() {
                @Override
                public boolean filter(EPGMEdge edge) {
                    return edge.getSourceId().equals(vertexId);
                }
            });
        } else {
            edges = graph.getEdges().filter(new FilterFunction<EPGMEdge>() {
                @Override
                public boolean filter(EPGMEdge edge) {
                    return edge.getTargetId().equals(vertexId);
                }
            });
        }

        // Extract the IDs of the other vertices (i.e., the neighbors)
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

        // Map the IDs back to the actual vertex objects
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
     * dfs方法是递归实现的。这个方法开始于一个顶点（起点），然后访问该顶点的所有邻居，如果邻居节点没有被访问过，就从那个节点继续深度搜索。
     * 在这个过程中，我们维护了一个访问过的顶点集合visited，避免重复访问。我们也维护一个路径列表path，记录当前路径。
     * @param graph
     * @param startVertexId
     * @param vertexId
     * @param visited
     * @param path
     * @param forwardSearch
     * @throws Exception
     */
    public void dfs(LogicalGraph graph, GradoopId startVertexId, GradoopId vertexId, Set<GradoopId> visited, LinkedList<GradoopId> path, boolean forwardSearch) throws Exception {
        visited.add(vertexId);
        path.add(vertexId);

        // Print the current path
        System.out.println("Current path: " + path);

        DataSet<EPGMVertex> neighbors = getNeighbors(graph, vertexId, forwardSearch);

        for(EPGMVertex neighbor : neighbors.collect()) {
            GradoopId neighborId = neighbor.getId();
            if (!visited.contains(neighborId)) {
                if (forwardSearch && neighborId.compareTo(startVertexId) > 0 || !forwardSearch && neighborId.compareTo(startVertexId) < 0) {
                    dfs(graph, startVertexId, neighborId, visited, path, forwardSearch);
                }
            }
        }

        path.remove(vertexId);
    }

    /**
     * search方法是深度优先搜索的启动方法。它初始化visited和path集合，并调用dfs方法开始搜索。
     * 它也接收一个forwardSearch参数，决定搜索的方向（向前或向后）。
     * @param graph
     * @param startVertexId
     * @param forwardSearch
     * @throws Exception
     */
    public void search(LogicalGraph graph, GradoopId startVertexId, boolean forwardSearch) throws Exception {
        Set<GradoopId> visited = new HashSet<>();
        LinkedList<GradoopId> path = new LinkedList<>();
        dfs(graph, startVertexId, startVertexId, visited, path, forwardSearch);
    }

    public void bfs(LogicalGraph graph, GradoopId startVertexId, boolean forwardSearch) throws Exception {
        Set<GradoopId> visited = new HashSet<>();
        Queue<GradoopId> queue = new LinkedList<>();

        Map<GradoopId, LinkedList<GradoopId>> paths = new HashMap<>();
        LinkedList<GradoopId> startPath = new LinkedList<>();
        startPath.add(startVertexId);
        paths.put(startVertexId, startPath);

        visited.add(startVertexId);
        queue.add(startVertexId);

        while (!queue.isEmpty()) {
            GradoopId currentVertexId = queue.poll();

            LinkedList<GradoopId> currentPath = paths.get(currentVertexId);
            System.out.println("Current path: " + currentPath);

            DataSet<EPGMVertex> neighbors = getNeighbors(graph, currentVertexId, forwardSearch);

            for(EPGMVertex neighbor : neighbors.collect()) {
                GradoopId neighborId = neighbor.getId();
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


    public void searchBFS(LogicalGraph graph, GradoopId startVertexId, boolean forwardSearch) throws Exception {
        bfs(graph, startVertexId, forwardSearch);
    }

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

        String inputPath = new ClassPathResource("timeSeriesGraph-20230427").getFile().getAbsolutePath();
        CSVDataSource dataSource = new CSVDataSource(inputPath, config);

        LogicalGraph inputGraph = dataSource.getLogicalGraph();
        LogicalGraph graph = GraphTransformer.transformGraph(env,inputGraph);

        //graph.getVertices().print();

        BetweennessCalculator calculator = new BetweennessCalculator(graph);
        Map<GradoopId, Double> scores = calculator.calculateWithSmallGraph();

        List<Map.Entry<GradoopId, Double>> sortedScores = new ArrayList<>(scores.entrySet());

        sortedScores.sort((entry1, entry2) -> entry2.getValue().compareTo(entry1.getValue()));

        Map.Entry<GradoopId, Double> highestScoreEntry = sortedScores.get(0);
        System.out.println("Vertex with the highest Betweenness Centrality: ID = " + highestScoreEntry.getKey() + ", Score = " + highestScoreEntry.getValue());

        GraphSearcher searcher = new GraphSearcher();
        searcher.search(graph, highestScoreEntry.getKey(), true); // Forward DFS search
        searcher.search(graph, highestScoreEntry.getKey(), false); // Backward DFS search

        searcher.searchBFS(graph, highestScoreEntry.getKey(), true); // Forward BFS search
        searcher.searchBFS(graph, highestScoreEntry.getKey(), false); // Backward BFS search
    }
}


