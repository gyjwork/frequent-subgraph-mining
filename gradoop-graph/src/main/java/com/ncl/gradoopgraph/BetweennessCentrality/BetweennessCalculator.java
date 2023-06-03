package com.ncl.gradoopgraph.BetweennessCentrality;

import com.ncl.gradoopgraph.utils.GraphTransformer;
import com.ncl.gradoopgraph.utils.GraphUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.jgrapht.Graph;
import org.jgrapht.alg.scoring.BetweennessCentrality;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.springframework.core.io.ClassPathResource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author gyj
 * @title: BetweennessCentrality
 * @projectName gradoop-graph
 * @description: TODO
 * @date 5/19/237:10 PM
 */
public class BetweennessCalculator {

    private Graph<GradoopId, DefaultEdge> graph;


    public BetweennessCalculator(LogicalGraph logicalGraph) {
        // convert LogicalGraph to JGraphT Graph
        this.graph = convertLogicalGraphToJGraphT(logicalGraph);
    }

    private Graph<GradoopId, DefaultEdge> convertLogicalGraphToJGraphT(LogicalGraph logicalGraph) {
        Graph<GradoopId, DefaultEdge> graph = new DefaultDirectedGraph<>(DefaultEdge.class);

        // add vertices
        try {
            logicalGraph.getVertices().collect().forEach(vertex -> graph.addVertex(vertex.getId()));
        } catch (Exception e) {
            e.printStackTrace();
        }

        // add edges
        try {
            logicalGraph.getEdges().collect().forEach(edge -> graph.addEdge(edge.getSourceId(), edge.getTargetId()));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return graph;
    }

    public Map<GradoopId, Double> calculateWithSmallGraph() {
        BetweennessCentrality<GradoopId, DefaultEdge> betweennessCentrality = new BetweennessCentrality<>(this.graph);
        Map<GradoopId, Double> scores = betweennessCentrality.getScores();
        return scores;
    }

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

        String inputPath = new ClassPathResource("timeSeriesGraph-20230427").getFile().getAbsolutePath();
        CSVDataSource dataSource = new CSVDataSource(inputPath, config);

        LogicalGraph inputGraph = dataSource.getLogicalGraph();
        LogicalGraph graph = GraphTransformer.transformGraph(env,inputGraph);

        BetweennessCalculator calculator = new BetweennessCalculator(graph);
        Map<GradoopId, Double> scores = calculator.calculateWithSmallGraph();

        List<Map.Entry<GradoopId, Double>> sortedScores = new ArrayList<>(scores.entrySet());

        sortedScores.sort((entry1, entry2) -> entry2.getValue().compareTo(entry1.getValue()));

        sortedScores.forEach(entry -> System.out.println("Vertex ID: " + entry.getKey() + ", Betweenness Centrality: " + entry.getValue()));

        // Get the node with highest betweenness centrality score
        if (!sortedScores.isEmpty()) {
            Map.Entry<GradoopId, Double> highestScoreEntry = sortedScores.get(0);
            System.out.println("Vertex with the highest Betweenness Centrality: ID = " + highestScoreEntry.getKey() + ", Score = " + highestScoreEntry.getValue());
            System.out.println(GraphUtils.getVertexById(graph, highestScoreEntry.getKey()).collect().get(0));
        } else {
            System.out.println("No nodes in the graph.");
        }
    }

}

