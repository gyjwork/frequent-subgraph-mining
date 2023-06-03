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
     * getNeighbors方法返回给定顶点的邻居节点集合。它首先找到所有与给定顶点相连的边，然后从这些边中提取出邻居节点的ID，最后通过ID获取邻居节点。
     * @param graph
     * @param vertexId
     * @return
     * @throws Exception
     */
    public DataSet<EPGMVertex> getNeighbors(LogicalGraph graph, GradoopId vertexId, SearchType type) throws Exception {
        DataSet<EPGMEdge> edges = graph.getEdges().filter(new FilterFunction<EPGMEdge>() {
            @Override
            public boolean filter(EPGMEdge edge) {
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
    public void dfs(LogicalGraph graph, GradoopId startVertexId, GradoopId vertexId, Set<GradoopId> visited, LinkedList<GradoopId> path, SearchType type) throws Exception {
        visited.add(vertexId);
        path.add(vertexId);
        System.out.println("Current path: " + path);

        DataSet<EPGMVertex> neighbors = getNeighbors(graph, vertexId, type);

        for (EPGMVertex neighbor : neighbors.collect()) {
            GradoopId neighborId = neighbor.getId();
            if (!visited.contains(neighborId)) {
                dfs(graph, startVertexId, neighborId, visited, path, type);
            }
        }

        path.remove(vertexId);
    }

    public void bfs(LogicalGraph graph, GradoopId startVertexId, SearchType type) throws Exception {
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

            DataSet<EPGMVertex> neighbors = getNeighbors(graph, currentVertexId, type);

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

    public void searchDFS(LogicalGraph graph, GradoopId startVertexId, SearchType type) throws Exception {
        Set<GradoopId> visited = new HashSet<>();
        LinkedList<GradoopId> path = new LinkedList<>();
        dfs(graph, startVertexId, startVertexId, visited, path, type);
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
        LogicalGraph graph = GraphTransformer.transformGraph(env,inputGraph);

        //graph.getVertices().print();

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


