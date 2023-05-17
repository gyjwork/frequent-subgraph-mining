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
        // 创建图集合
        Collection<Graph<String, String>> graphs = new ArrayList<>();

        // 创建图
        ListGraph<String, String> graph = new ListGraph<>();

        // 创建三个相同的子图
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


        // 添加到图集合
        graphs.add(graph);
        graphs.add(graph2);

        // 创建设置对象
        Settings<String, String> settings = new Settings<>();

        // 设置频繁子图的最小频率为2
        settings.minFreq = new IntFrequency(2);

        // 设置算法为 gSpan
        settings.algorithm = new de.parsemis.algorithms.gSpan.Algorithm<>();

        // 设置图的工厂类，这里假设你是使用字符串作为节点和边的类型
        settings.factory = new ListGraph.Factory<>(new StringLabelParser(), new StringLabelParser());

        settings.strategy = new de.parsemis.strategy.BFSStrategy<>();

        settings.graphs = graphs;

        // 执行挖掘操作
        Collection<Fragment<String, String>> fragments = Miner.mine(graphs, settings);

        // 打印原始图
        System.out.println("Original Graph:");
        printGraph(graph);

        // 打印频繁子图
        System.out.println("Frequent Subgraphs:");
        for (Fragment<String, String> fragment : fragments) {
            Graph<String, String> subgraph = fragment.toGraph();
            printGraph(subgraph);
        }
    }

    private static void printGraph(Graph<String, String> graph) {
        // 使用迭代器遍历节点
        Iterator<Node<String, String>> nodeIterator = graph.nodeIterator();
        while (nodeIterator.hasNext()) {
            Node<String, String> node = nodeIterator.next();
            System.out.println("Node: " + node.getLabel());
        }

        // 使用迭代器遍历边
        Iterator<Edge<String, String>> edgeIterator = graph.edgeIterator();
        while (edgeIterator.hasNext()) {
            Edge<String, String> edge = edgeIterator.next();
            System.out.println("Edge: " + edge.getLabel());
        }
    }
}
