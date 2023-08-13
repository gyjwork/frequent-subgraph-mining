package com.ncl.gradoopgraph.Algorithms;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author gyj
 * @title: FindAllSimplePaths
 * @projectName gradoop-graph
 * @description: TODO
 * @date 8/10/233:38 AM
 */
public class FindAllSimplePaths {

    public static Map<String, Map<GradoopId, Integer>> run(LogicalGraph graph, List<EPGMVertex> vertices) throws Exception {

        // 创建两个 Map 类型的变量，分别用来存储正向邻接表和反向邻接表
        // Used to store forward and reverse adjacency tables
        Map<GradoopId, List<GradoopId>> adjList = new HashMap<>();
        Map<GradoopId, List<GradoopId>> invAdjList = new HashMap<>();

        // 创建一个 int 数组，该数组仅有一个元素，用于记录下一个可用的路径 id
        // Used to record the next available path id
        int[] pathIdCounter = new int[1];

        // 遍历图中的所有边，构建正向邻接表和反向邻接表
        // Iterate over all edges in the graph and construct forward and reverse neighbourhood tables
        for (EPGMEdge edge : graph.getEdges().collect()) {
            GradoopId sourceId = edge.getSourceId();
            GradoopId targetId = edge.getTargetId();
            adjList.computeIfAbsent(sourceId, k -> new ArrayList<>()).add(targetId);
            invAdjList.computeIfAbsent(targetId, k -> new ArrayList<>()).add(sourceId);
        }

        // 找出所有没有入边的节点，这些节点将作为路径的起始节点
        // Find all the nodes that are not in the edges, these nodes will be used as the starting nodes of the path
        List<EPGMVertex> startingVertices = vertices.stream()
                .filter(vertex -> {
                    List<GradoopId> ls = invAdjList.get(vertex.getId());
                    return ls == null || ls.isEmpty();
                })
                .collect(Collectors.toList());

        // 找出所有没有出边的节点，这些节点将作为路径的终止节点
        // Find all the nodes that are not out of the edge, these nodes will be used as the terminating nodes of the path
        Set<GradoopId> terminalVertices = vertices.stream()
                .filter(vertex -> {
                    List<GradoopId> ls = adjList.get(vertex.getId());
                    return ls == null || ls.isEmpty();
                })
                .map(EPGMVertex::getId)
                .collect(Collectors.toSet());

        // 遍历所有的起始节点，对每个节点调用 computeShortestPaths 方法来计算从该节点出发的所有最短路径
        // Iterate over all the start nodes and call the computeShortestPaths method on each node to compute all the shortest paths from that node.
        Map<String, Map<GradoopId, Integer>> shortestPaths = new HashMap<>();
        for (EPGMVertex vertex : startingVertices) {
            GradoopId sourceId = vertex.getId();
            Map<String, Map<GradoopId, Integer>> shortestPath = computeShortestPaths(sourceId, new ArrayList<>(vertices), adjList, terminalVertices, pathIdCounter);
            shortestPaths.putAll(shortestPath);
            //System.out.println("Shortest paths from vertex " + sourceId + ":");

            // 打印出从当前节点出发的所有最短路径
            // Print out all the shortest paths from the current node
//            for (Map.Entry<String, Map<GradoopId, Integer>> entry : shortestPath.entrySet()) {
//                System.out.println("Path id " + entry.getKey() + " to vertex " + entry.getValue().keySet().stream().max(Comparator.comparing(entry.getValue()::get)).get() + ":");
//                for (GradoopId id : entry.getValue().keySet()) {
//                    System.out.println(getVertexById(vertices, id));
//                }
//                System.out.println();
//            }
        }

        return shortestPaths;
    }

    /*
    这个方法的设计目的是为了计算从一个给定的源节点到所有其他节点的最短路径。方法使用了Dijkstra的算法。
    在Dijkstra的算法中，我们从源节点开始，逐步“访问”所有的节点，并且在每一步中都保证我们已经找到了到目前为止访问过的节点的最短路径。
    源节点到各个终止节点的最短路径以映射的形式返回，其中键是由路径ID和路径计数器组成的字符串，值是一个映射，该映射的键是节点的ID，值是该节点在路径中的位置。
     */
    private static Map<String, Map<GradoopId, Integer>> computeShortestPaths(GradoopId sourceId, List<EPGMVertex> sortedVertices,
                                                                            Map<GradoopId, List<GradoopId>> adjList,
                                                                            Set<GradoopId> terminalVertices, int[] pathIdCounter) {
        // 初始化一个映射，用于存储每个节点到源节点的最短距离
        // Initialise a mapping to store the shortest distance from each node to the source node
        Map<GradoopId, Integer> distances = new HashMap<>();

        // 初始化一个映射，用于存储每个节点在最短路径中的前一个节点
        // Initialise a map to store the previous node in the shortest path for each node
        Map<GradoopId, GradoopId> previousNodes = new HashMap<>();

        // 初始化一个映射，用于存储找到的最短路径
        // Initialise a map to store the shortest paths found
        Map<String, Map<GradoopId, Integer>> shortestPaths = new HashMap<>();

        // 初始化一个队列，用于存储待访问的节点
        // Initialise a queue to store the nodes to be accessed
        Queue<GradoopId> toVisit = new LinkedList<>();

        // 对于所有节点，初始化其到源节点的距离和前驱节点
        // For all nodes, initialise their distance to the source node and the precursor node
        for (EPGMVertex vertex : sortedVertices) {
            GradoopId id = vertex.getId();
            // 如果是源节点，距离设置为0，并加入待访问队列
            // If it is a source node, the distance is set to 0 and added to the pending access queue
            if (id.equals(sourceId)) {
                distances.put(id, 0);
                toVisit.add(id);
            } else {
                // 对于其它节点，距离初始化为无穷大
                // For other nodes, the distance is initialised to infinity
                distances.put(id, Integer.MAX_VALUE);
            }
            // 初始化前驱节点为null
            // Initialise the predecessor node to null
            previousNodes.put(id, null);
        }

        // 当待访问队列非空时，继续执行循环
        // When the queue to be accessed is not empty, continue the execution of the loop
        while (!toVisit.isEmpty()) {
            // 取出待访问队列的第一个节点
            // Take the first node of the queue to be accessed
            GradoopId id = toVisit.remove();

            // 如果该节点有邻居节点
            // If the node has neighbouring nodes
            if (adjList.containsKey(id)) {
                // 遍历邻居节点
                // Iterate over neighbouring nodes
                for (GradoopId neighbourId : adjList.get(id)) {
                    // 计算新的距离（当前节点的距离加1）
                    // Calculate new distance (current node's distance plus 1)
                    int altDistance = distances.get(id) + 1;

                    // 如果新的距离小于邻居节点的当前距离
                    // If the new distance is less than the current distance of the neighbouring nodes
                    if (altDistance < distances.get(neighbourId)) {
                        // 更新邻居节点的距离和前驱节点，并将邻居节点加入待访问队列
                        // Update the distance and antecedent nodes of the neighbouring nodes and add the neighbouring nodes to the pending access queue
                        distances.put(neighbourId, altDistance);
                        previousNodes.put(neighbourId, id);
                        toVisit.add(neighbourId);
                    }
                }
            }
        }

        // 对于所有节点，如果它是终止节点
        // For all nodes, if it is a terminating node
        for (GradoopId id : distances.keySet()) {
            if (terminalVertices.contains(id)) {
                // 初始化一个列表，用于存储从源节点到该终止节点的最短路径
                // Initialize a list to store the shortest path from the source node to the terminal node
                List<GradoopId> path = new ArrayList<>();

                // 初始化一个映射，用于存储路径中每个节点的位置
                // Initialize a map for storing the position of each node in the path
                Map<GradoopId, Integer> nodePositions = new HashMap<>();
                GradoopId currentNode = id;
                int position = 0;

                // 通过反向追踪前驱节点，找出最短路径
                // Find the shortest path by backtracking the predecessor node
                while (currentNode != null) {
                    path.add(0, currentNode);
                    nodePositions.put(currentNode, position++);
                    currentNode = previousNodes.get(currentNode);
                }

                // 将找到的最短路径添加到最短路径映射中
                // Add the shortest path found to the shortest path map
                shortestPaths.put("path" + pathIdCounter[0]++, nodePositions);

                // 调整路径中每个节点的位置，使得源节点的位置是0，终止节点的位置是路径长度减一
                // Adjust the position of each node in the path so that the position of the source node is 0 and the position of the termination node is the length of the path minus one
                for (GradoopId nodeId : nodePositions.keySet()) {
                    nodePositions.put(nodeId, path.size() - nodePositions.get(nodeId) - 1);
                }
            }
        }

        return shortestPaths;
    }
}
