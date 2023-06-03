package com.ncl.gradoopgraph.BetweennessCentrality;

import org.jgrapht.Graph;
import org.jgrapht.alg.scoring.BetweennessCentrality;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;

/**
 * @author gyj
 * @title: Jgrapht
 * @projectName gradoop-graph
 * @description: TODO
 * @date 5/19/237:56 PM
 */
public class Jgrapht {
    public static void main(String[] args) {
        // Create a graph
        Graph<String, DefaultEdge> graph = new SimpleGraph<>(DefaultEdge.class);
        graph.addVertex("A");
        graph.addVertex("B");
        graph.addVertex("C");
        graph.addEdge("A", "B");
        graph.addEdge("B", "C");

        BetweennessCentrality<String, DefaultEdge> betweennessCentrality = new BetweennessCentrality<>(graph);
        for (String vertex : graph.vertexSet()) {
            System.out.println("Betweenness centrality of " + vertex + " = " + betweennessCentrality.getVertexScore(vertex));
        }
    }
}
