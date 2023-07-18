package com.ncl.gradoopgraph.utils;

import com.ncl.gradoopgraph.Beans.GeneralFrequentPath;
import com.ncl.gradoopgraph.Beans.SimplePath;
import javafx.util.Pair;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author gyj
 * @title: GeneralFrequentGraph
 * @projectName gradoop-graph
 * @description: final algorithm, search general frequent paths
 * @date 7/17/23 3:21 PM
 */
public class GeneralFrequentGraph {

    private final ExecutionEnvironment env;

    public GeneralFrequentGraph(ExecutionEnvironment env) {
        this.env = env;
    }

    public List<GeneralFrequentPath> miningAlgorithm(LogicalGraph graph, int minSupport) throws Exception {
        // 1. Perform topological sort on the graph
        DataSet<EPGMVertex> topologicalSort = TopologicalSort.topologicalSort(graph, env);
        Collection<EPGMVertex> vertices = topologicalSort.collect();

        // 2. Find all simple paths
        Map<String, Map<GradoopId, Integer>> simplePaths = findAllSimplePaths(graph, topologicalSort);

        // 3. 创建两个 Map 类型的变量，分别用来存储每个标签对应的顶点集合，以及每个顶点对应的路径集合
        Map<String, Set<GradoopId>> labelToVertices = new HashMap<>();
        Map<GradoopId, Map<String, Integer>> vertexToPaths = new HashMap<>();

        for (Map.Entry<String, Map<GradoopId, Integer>> entry : simplePaths.entrySet()) {
            Map<GradoopId, Integer> path = entry.getValue();

            for (GradoopId id : path.keySet()) {
                EPGMVertex vertex = getVertexById(vertices, id);
                Integer pathValue = path.get(id);

                labelToVertices.computeIfAbsent(vertex.getLabel(), k -> new HashSet<>()).add(id);
                vertexToPaths.computeIfAbsent(id, k -> new HashMap<>()).put(entry.getKey(), pathValue);
            }
        }

        // 4. 提取所有的label
        Set<String> labels = labelToVertices.keySet();
        List<String[]> labelPairs = new ArrayList<>();

        for (String label1 : labels) {
            for (String label2 : labels) {
                if (!label1.equals(label2)) {
                    labelPairs.add(new String[]{label1, label2});
                }
            }
        }

        List<GeneralFrequentPath> result = new ArrayList<>();

        HashSet<SimplePath> allPaths = new HashSet<>();

        for (String[] labelPair : labelPairs) {
            String startLabel = labelPair[0];
            String endLabel = labelPair[1];
            HashSet<SimplePath> paths = findPaths(startLabel, endLabel, labelToVertices, vertexToPaths, minSupport);
            allPaths.addAll(paths);
        }

        return result;
    }

    // Auxiliary method for getting the vertices with a given ID from the dataset
    private static EPGMVertex getVertexById(Collection<EPGMVertex> vertices, GradoopId id) {
        for (EPGMVertex vertex : vertices) {
            if (vertex.getId().equals(id)) {
                return vertex;
            }
        }
        return null;
    }


    public static Map<String, Map<GradoopId, Integer>> findAllSimplePaths(LogicalGraph graph, DataSet<EPGMVertex> topologicalSort) throws Exception {

        Collection<EPGMVertex> vertices = topologicalSort.collect();

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

//        // 创建两个 Map 类型的变量，分别用来存储每个标签对应的顶点集合，以及每个顶点对应的路径集合
//        // Used to store the set of vertices corresponding to each label, and the set of paths corresponding to each vertex
//        Map<String, Set<GradoopId>> labelToVertices = new HashMap<>();
//        Map<GradoopId, Map<String, Integer>> vertexToPaths = new HashMap<>();

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
//            System.out.println("Shortest paths from vertex " + sourceId + ":");
//
//            // 打印出从当前节点出发的所有最短路径
//            // Print out all the shortest paths from the current node
//            for (Map.Entry<String, Map<GradoopId, Integer>> entry : shortestPaths.entrySet()) {
//                System.out.println("Path id " + entry.getKey() + " to vertex " + entry.getValue().keySet().stream().max(Comparator.comparing(entry.getValue()::get)).get() + ":");
//                for (GradoopId id : entry.getValue().keySet()) {
//                    System.out.println(getVertexById(vertices, id));
//                    labelToVertices.computeIfAbsent(getVertexById(vertices, id).getLabel(), k -> new HashSet<>()).add(id);
//                    vertexToPaths.computeIfAbsent(id, k -> new HashMap<>()).put(entry.getKey(), entry.getValue().get(id));
//                }
//                System.out.println();
//            }
        }
        // 打印出标签到顶点集合和顶点到路径集合的映射
        // Print out the mapping of labels to vertex collections and vertices to path collections
        //System.out.println("Label to Vertices mapping: " + labelToVertices);
        //System.out.println();
        //System.out.println("Vertex to Paths mapping: " + vertexToPaths);

        // 找到从"buys"到"sells"的所有路径，这些路径在整个数据集中的出现次数至少为2
        // Find all paths from "buys" to "sells" that have at least 2 occurrences in the entire dataset.
        //findPaths("buys", "transfers_ownership", labelToVertices, vertexToPaths, 2);
        return shortestPaths;
    }


    /*
    这个方法的设计目的是为了计算从一个给定的源节点到所有其他节点的最短路径。方法使用了Dijkstra的算法。
    在Dijkstra的算法中，我们从源节点开始，逐步“访问”所有的节点，并且在每一步中都保证我们已经找到了到目前为止访问过的节点的最短路径。
    源节点到各个终止节点的最短路径以映射的形式返回，其中键是由路径ID和路径计数器组成的字符串，值是一个映射，该映射的键是节点的ID，值是该节点在路径中的位置。
     */
    public static Map<String, Map<GradoopId, Integer>> computeShortestPaths(GradoopId sourceId, List<EPGMVertex> sortedVertices,
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

    /*

    这个方法使用了集合操作来找出所有满足给定条件的路径，这些条件包括：路径的起始和终止节点的标签，以及路径的最小支持度。
    这个方法首先通过遍历起始和终止节点对，然后通过找出这两个节点之间的所有路径来找到满足条件的路径。
    然后，这个方法通过计数和比较这些路径的数量与给定的最小支持度来确定哪些路径应该被添加到结果集中。

    This method uses a set operation to find all paths that satisfy the given conditions,
    which include: the labels of the start and end nodes of the path, and the minimum support of the path.
    This method first finds the paths that satisfy the conditions by traversing the start and termination node pairs
    and then by finding all the paths between these two nodes.
    The method then determines which paths should be added to the result set by counting
    and comparing the number of these paths with the given minimum support.

     */
    public static HashSet<SimplePath> findPaths(String startLabel, String endLabel,
                                                Map<String, Set<GradoopId>> labelToVertices,
                                                Map<GradoopId, Map<String, Integer>> vertexToPaths, int minSupport) {

        // 创建一个 HashSet 用于存储满足条件的一般路径
        // Used to store general paths that satisfy conditions
        HashSet<SimplePath> generalPaths = new HashSet<>();

        // 创建一个 Set 用于避免重复计数的路径
        // Paths for avoiding double counting
        Set<String> countedPaths = new HashSet<>();

        // 获取起始标签和终止标签下的节点
        // Get the nodes under the start and end labels
        Set<GradoopId> startVertices = labelToVertices.get(startLabel);
        Set<GradoopId> endVertices = labelToVertices.get(endLabel);

        int count = 0;

        // 对每一对起始节点和终止节点，找出它们在同一条路径上，且起始节点在终止节点前面的所有路径
        // For each pair of start and end nodes, find all the paths where they are on the same path and the start node is in front of the end node
        for (GradoopId ids : startVertices) {
            Map<String, Integer> paths = vertexToPaths.get(ids);
            for (GradoopId ide : endVertices) {
                Map<String, Integer> pathe = vertexToPaths.get(ide);
                Set<String> intersection = new HashSet<>(paths.keySet());
                // retainAll 方法用于获取两个集合的交集
                // Get the intersection of two sets
                intersection.retainAll(pathe.keySet());
                for (String path : intersection) {
                    // 如果路径中起始节点的位置小于终止节点的位置，且这条路径还未被计数过，那么增加计数器的值并将这条路径添加到已计数路径的集合中
                    // If the position of the start node in the path is less than the position of the end node, and the path has not been counted yet,
                    // then increase the value of the counter and add the path to the set of counted paths
                    if (paths.get(path) < pathe.get(path) && !countedPaths.contains(path)) {
                        count++;
                        countedPaths.add(path);
                    }
                }
                // 打印出两个节点的共享路径的数量
                // Print out the number of shared paths for both nodes
                System.out.println("Number of common elements: " + intersection.size());
            }
        }

        // 如果满足条件的路径数大于或等于最小支持度，那么将这条路径添加到结果集中
        // If the number of paths that satisfy the condition is greater than or equal to the minimum support,
        // then add this path to the result set
        if (count >= minSupport) {
            generalPaths.add(new SimplePath(startLabel, endLabel));
        }

        // 打印出满足条件的路径的数量和标签
        // Print out the number and labels of paths that satisfy the condition
        System.out.println(new SimplePath(startLabel, endLabel).toString() + " number : " + count);

        generalPaths.forEach(System.out::println);

        return generalPaths;

    }


}
