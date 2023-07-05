package com.ncl.gradoopgraph.utils;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.springframework.core.io.ClassPathResource;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author gyj
 * @title: GraphSearch
 * @projectName gradoop-graph
 * @description: Use TopologicalSort to search all sinple paths
 * @date 6/25/235:31 PM
 */
public class GraphSearch {
    // 辅助方法，用于从数据集中获取给定ID的顶点
    // Auxiliary method for getting the vertices with a given ID from the dataset
    private static EPGMVertex getVertexById(Collection<EPGMVertex> vertices, GradoopId id) throws Exception {
        for (EPGMVertex vertex : vertices) {
            if (vertex.getId().equals(id)) {
                return vertex;
            }
        }
        return null;
    }

    /**
    public static void printAllPaths(LogicalGraph graph, DataSet<EPGMVertex> sortedVertices) throws Exception {
        Collection<EPGMVertex> vertices = sortedVertices.collect();

        // 邻接列表，用于表示图的结构。键为顶点的ID，值为该顶点的所有邻接顶点的ID列表
        // Adjacency list, used to represent the structure of the graph. The key is the ID of the vertex,
        // the value is the list of IDs of all neighbouring vertices of that vertex
        Map<GradoopId, List<GradoopId>> adjList = new HashMap<>();
        Map<GradoopId, List<GradoopId>> invAdjList = new HashMap<>();

        // 遍历图的所有边，构造邻接列表
        // Iterate over all edges of the graph to construct the adjacency list
        for (EPGMEdge edge : graph.getEdges().collect()) {
            GradoopId sourceId = edge.getSourceId();
            GradoopId targetId = edge.getTargetId();
            if (!adjList.containsKey(sourceId)) {
                adjList.put(sourceId, new ArrayList<>());
            }
            if (!invAdjList.containsKey(targetId)) {
                invAdjList.put(targetId, new ArrayList<>());
            }
            adjList.get(sourceId).add(targetId);
            invAdjList.get(targetId).add(sourceId);
        }

        // 用于计数顶点的映射。键为顶点的标签，值为该顶点在所有路径中出现的次数
        // A map to count the vertices. The key is the label of the vertex and the value is the number of times the vertex appears in all paths
        Map<String, Integer> vertexCount = new HashMap<>();

        // 获取所有没有入边的顶点，也就是可能作为搜索开始的顶点
        // Get all vertices that do not have an incoming edge, i.e. vertices that could be the start of the search
        List<EPGMVertex> startingVertices = vertices.stream()
                .filter(vertex -> {
                    List<GradoopId> ls = invAdjList.get(vertex.getId());
                    return ls == null || ls.isEmpty();
                })
                .collect(Collectors.toList());

        // 遍历所有可能的起始顶点
        // Iterate over all possible starting vertices
        for (EPGMVertex vertex : startingVertices) {
            // 当前路径，用于深度优先搜索
            // Current path, for depth-first search
            List<GradoopId> initialPath = new ArrayList<>();
            // 所有的路径，每个元素都是一个从当前顶点开始的路径
            // All paths, each element being a path from the current vertex
            List<List<GradoopId>> allPaths = new ArrayList<>();
            // 已访问的顶点，用于避免在搜索过程中重复访问同一个顶点
            // Visited vertices, to avoid repeated visits to the same vertex during the search
            Set<GradoopId> visited = new HashSet<>();
            dfs(vertex.getId(), initialPath, adjList, allPaths, visited);
            System.out.println("Paths starting from vertex " + vertex.getId() + ":");
            for (List<GradoopId> path : allPaths) {
                System.out.println("Path:");
                for (GradoopId id : path) {
                    // 增加顶点的计数，使用顶点标签代替顶点ID
                    EPGMVertex currentVertex = getVertexById(vertices, id);
                    String label = currentVertex.getLabel();
                    vertexCount.put(label, vertexCount.getOrDefault(label, 0) + 1);
                    System.out.println(currentVertex);
                }
                System.out.println();
            }
        }


        // 打印顶点的计数结果，使用顶点标签代替顶点ID
        System.out.println("Vertex counts:");
        for (Map.Entry<String, Integer> entry : vertexCount.entrySet()) {
            System.out.println("Vertex " + entry.getKey() + " appears " + entry.getValue() + " times.");
        }
    }
     */

    public static void printAllPaths(LogicalGraph graph, DataSet<EPGMVertex> sortedVertices) throws Exception {
        // 将DataSet转换为Java集合，便于操作
        // Convert DataSet to a Java collection for easy manipulation
        Collection<EPGMVertex> vertices = sortedVertices.collect();

        // 创建邻接列表和逆邻接列表
        // Create an adjacency list and an inverse adjacency list
        Map<GradoopId, List<GradoopId>> adjList = new HashMap<>();
        Map<GradoopId, List<GradoopId>> invAdjList = new HashMap<>();

        // 遍历所有边，为邻接列表和逆邻接列表添加条目
        // Iterate over all edges and add entries to the adjacency list and the inverse adjacency list
        for (EPGMEdge edge : graph.getEdges().collect()) {
            GradoopId sourceId = edge.getSourceId();
            GradoopId targetId = edge.getTargetId();
            adjList.computeIfAbsent(sourceId, k -> new ArrayList<>()).add(targetId);
            invAdjList.computeIfAbsent(targetId, k -> new ArrayList<>()).add(sourceId);
        }

        // 获取所有没有入边的顶点，作为搜索开始的顶点
        // Get all vertices that have no incoming edges, to be used as starting points for the search
        List<EPGMVertex> startingVertices = vertices.stream()
                .filter(vertex -> {
                    List<GradoopId> ls = invAdjList.get(vertex.getId());
                    return ls == null || ls.isEmpty();
                })
                .collect(Collectors.toList());

        // 遍历所有可能的起始顶点
        // Iterate over all potential starting vertices
        for (EPGMVertex vertex : startingVertices) {
            // 对每个起始顶点，计算从该顶点出发的所有最短路径
            // For each starting vertex, compute all shortest paths starting from it
            Map<GradoopId, List<GradoopId>> shortestPaths = computeShortestPaths(vertex.getId(), new ArrayList<>(vertices), adjList);

            // 打印所有的最短路径
            // Print all the shortest paths
            System.out.println("Shortest paths from vertex " + vertex.getId() + ":");
            for (Map.Entry<GradoopId, List<GradoopId>> entry : shortestPaths.entrySet()) {
                System.out.println("Path to vertex " + entry.getKey() + ":");
                for (GradoopId id : entry.getValue()) {
                    System.out.println(getVertexById(vertices, id));
                }
                System.out.println();
            }
        }
    }



    public static void dfs(GradoopId currentId, List<GradoopId> currentPath, Map<GradoopId, List<GradoopId>> adjList, List<List<GradoopId>> allPaths, Set<GradoopId> visited) {
        currentPath.add(currentId);
        visited.add(currentId);

        // 如果当前顶点没有邻接顶点，或者所有邻接顶点都已经访问过，则当前路径是一条完整的路径，加入到allPaths中
        // If the current vertex has no neighbouring vertices, or if all neighbouring vertices have been visited,
        // then the current path is a complete path and is added to allPaths
        if (!adjList.containsKey(currentId) || adjList.get(currentId).isEmpty()) {
            allPaths.add(new ArrayList<>(currentPath));
        } else {
            // 对于每个邻接顶点，如果它还没有被访问过，就从它开始深度优先搜索
            // For each neighbouring vertex, if it has not already been visited, start a depth-first search from it
            for (GradoopId id : adjList.get(currentId)) {
                if (!visited.contains(id)) {
                    dfs(id, currentPath, adjList, allPaths, visited);
                }
            }
        }

        // 回溯过程中，移除当前顶点
        // Remove the current vertex during backtracking
        currentPath.remove(currentId);
        visited.remove(currentId);
    }

    public static Map<GradoopId, List<GradoopId>> computeShortestPaths(GradoopId sourceId, List<EPGMVertex> sortedVertices, Map<GradoopId, List<GradoopId>> adjList) {
        // 初始化距离、前置节点和最短路径的映射
        // Initialize the mapping of distances, previous nodes, and shortest paths
        Map<GradoopId, Integer> distances = new HashMap<>();
        Map<GradoopId, GradoopId> previousNodes = new HashMap<>();
        Map<GradoopId, List<GradoopId>> shortestPaths = new HashMap<>();

        // 对每个顶点，设置其到源点的距离，源点的距离为0，其他的为无穷大
        // For each vertex, set its distance to the source vertex, the distance of the source vertex is 0, others are infinity
        for (EPGMVertex vertex : sortedVertices) {
            GradoopId id = vertex.getId();
            distances.put(id, id.equals(sourceId) ? 0 : Integer.MAX_VALUE);
        }

        // 遍历所有的顶点，根据邻接节点更新距离和前置节点
        // Iterate over all vertices, update distances and previous nodes based on neighboring vertices
        for (EPGMVertex vertex : sortedVertices) {
            GradoopId id = vertex.getId();
            if (adjList.containsKey(id)) {
                for (GradoopId neighbourId : adjList.get(id)) {
                    int altDistance = distances.get(id) + 1;  // Assuming all edges have the same weight
                    if (altDistance < distances.get(neighbourId)) {
                        distances.put(neighbourId, altDistance);
                        previousNodes.put(neighbourId, id);
                    }
                }
            }
        }

        // 根据前置节点映射，构建最短路径
        // Build shortest paths based on the mapping of previous nodes
        for (GradoopId id : distances.keySet()) {
            if (!id.equals(sourceId)) {
                List<GradoopId> path = new ArrayList<>();
                GradoopId currentNode = id;
                while (currentNode != null) {
                    path.add(0, currentNode);
                    currentNode = previousNodes.get(currentNode);
                }
                shortestPaths.put(id, path);
            }
        }

        return shortestPaths;
    }


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

        String inputPath = new ClassPathResource("timeSeriesGraph-20230427").getFile().getAbsolutePath();
        CSVDataSource dataSource = new CSVDataSource(inputPath, config);

        LogicalGraph inputGraph = dataSource.getLogicalGraph();
        LogicalGraph graph = GraphTransformer.transformGraph(env, inputGraph);

        graph.print();

        DataSet<EPGMVertex> topologicalSort = TopologicalSort.topologicalSort(graph, env);
        topologicalSort.print();

        printAllPaths(graph, topologicalSort);
    }
}

