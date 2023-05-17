package com.ncl.gradoopgraph.findQ;

import java.util.*;


/**
 * @author gyj
 * @title: FrequentSubgraphMining
 * @projectName gradoop-graph
 * @description: TODO
 * @date 5/6/232:38 AM
 */

// 图类定义
class Graph {
    // 邻接列表用于存储图的结构
    Map<String, List<String>> adjacencyList;

    // 构造方法
    public Graph() {
        adjacencyList = new HashMap<>();
    }

    // 添加边的方法，source为源节点，destination为目标节点
    public void addEdge(String source, String destination) {
        adjacencyList.computeIfAbsent(source, k -> new ArrayList<>()).add(destination);
    }

    // 获取某个节点的所有邻居节点
    public List<String> getNeighbors(String node) {
        return adjacencyList.getOrDefault(node, Collections.emptyList());
    }
}

// 频繁子图挖掘类
public class FrequentSubgraphMining {
    public static void main(String[] args) {
        // 定义输入图
        Graph graph = new Graph();
        graph.addEdge("A", "B");
        graph.addEdge("B", "K");
        graph.addEdge("K", "S");
        graph.addEdge("A", "D");
        graph.addEdge("D", "F");
        graph.addEdge("A", "D");
        graph.addEdge("A", "D");
        graph.addEdge("D", "F");
        graph.addEdge("A", "D");
        graph.addEdge("D", "H");

        // 定义子图模式和最小支持阈值
        String[] subgraphPatterns = {"A->D", "A->B"};
        int minSupport = 3;

        // 执行频繁子图挖掘
        for (String pattern : subgraphPatterns) {
            int count = countSubgraphOccurrences(graph, pattern);
            // 如果子图模式出现的次数大于等于最小支持阈值，则打印结果
            if (count >= minSupport) {
                System.out.println("The subgraph pattern '" + pattern + "' appears " + count + " times.");
            }
        }
    }

    // 统计子图模式在图中出现的次数
    public static int countSubgraphOccurrences(Graph graph, String pattern) {
        // 将模式分割成节点
        String[] nodes = pattern.split("->");
        String startNode = nodes[0];

        Set<String> visited = new HashSet<>();
        Queue<String> queue = new LinkedList<>();
        queue.add(startNode);

        int count = 0;
        while (!queue.isEmpty()) {
            String currentNode = queue.poll();

            // 如果节点已经被访问过，跳过
            if (visited.contains(currentNode)) {
                continue;
            }
            visited.add(currentNode);

            // 获取当前节点的所有邻居
            List<String> neighbors = graph.getNeighbors(currentNode);
            if (!neighbors.isEmpty()) {
                for (String neighbor : neighbors) {
                    // 如果邻居节点是模式的最后一个节点，计数器加一
                    if (neighbor.equals(nodes[nodes.length - 1])) {
                        count++;
                    }
                    // 如果邻居节点还未被访问，加入到队列中
                    if (!visited.contains(neighbor)) {
                        queue.add(neighbor);
                    }
                }
            }
        }
        // 返回模式出现的次数
        return count;
    }
}


