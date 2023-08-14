package com.ncl.gradoopgraph.GSpan;

import de.parsemis.Miner;
import de.parsemis.graph.Edge;
import de.parsemis.graph.Graph;
import de.parsemis.graph.ListGraph;
import de.parsemis.graph.Node;
import de.parsemis.miner.environment.Settings;
import de.parsemis.miner.general.Fragment;
import de.parsemis.miner.general.IntFrequency;
import de.parsemis.parsers.StringLabelParser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

public class GSpanExample {
    public static void main(String[] args) {
        Collection<Graph<String, String>> graphs = new ArrayList<>();

        ListGraph<String, String> graph = new ListGraph<>();

        Node<String, String> nodeA = graph.addNode("A");
        Node<String, String> nodeB = graph.addNode("B");
        Node<String, String> nodeC = graph.addNode("C");
        Node<String, String> nodeD = graph.addNode("D");

        Node<String, String> nodeA2 = graph.addNode("A");
        Node<String, String> nodeB2 = graph.addNode("B");

        graph.addEdge(nodeA, nodeB, "A-B", 1);
        graph.addEdge(nodeB, nodeC, "B-C", 1);
        graph.addEdge(nodeC, nodeD, "C-D", 1);
        graph.addEdge(nodeD, nodeA2, "D-A", 1);
        graph.addEdge(nodeA2, nodeB2, "A-B", 1);

        ListGraph<String, String> graph2 = new ListGraph<>();
        Node<String, String> A2 = graph2.addNode("A");
        Node<String, String> B2 = graph2.addNode("B");
        graph2.addEdge(A2, B2, "A-B", 1);


        graphs.add(graph);
        graphs.add(graph2);

        Settings<String, String> settings = new Settings<>();

        settings.minFreq = new IntFrequency(2);

        settings.algorithm = new de.parsemis.algorithms.gSpan.Algorithm<>();

        settings.factory = new ListGraph.Factory<>(new StringLabelParser(), new StringLabelParser());

        settings.strategy = new de.parsemis.strategy.BFSStrategy<>();

        settings.graphs = graphs;

        Collection<Fragment<String, String>> fragments = Miner.mine(graphs, settings);

        System.out.println("Original Graph:");
        printGraph(graph);

        System.out.println("Frequent Subgraphs:");
        for (Fragment<String, String> fragment : fragments) {
            Graph<String, String> subgraph = fragment.toGraph();
            printGraph(subgraph);
        }
    }

    private static void printGraph(Graph<String, String> graph) {
        Iterator<Node<String, String>> nodeIterator = graph.nodeIterator();
        while (nodeIterator.hasNext()) {
            Node<String, String> node = nodeIterator.next();
            System.out.println("Node: " + node.getLabel());
        }

        Iterator<Edge<String, String>> edgeIterator = graph.edgeIterator();
        while (edgeIterator.hasNext()) {
            Edge<String, String> edge = edgeIterator.next();
            System.out.println("Edge: " + edge.getLabel());
        }
    }
}
