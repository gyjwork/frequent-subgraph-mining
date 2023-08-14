package com.ncl.gradoopgraph.utils;

import com.ncl.gradoopgraph.loadData.TestData;
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
 * @title: GraphSearch
 * @projectName gradoop-graph
 * @description: Use TopologicalSort to search all sinple paths
 * @date 6/25/235:31 PM
 */
public class GraphSearch {
    // Auxiliary method for getting the vertices with a given ID from the dataset
    private static EPGMVertex getVertexById(Collection<EPGMVertex> vertices, GradoopId id) {
        for (EPGMVertex vertex : vertices) {
            if (vertex.getId().equals(id)) {
                return vertex;
            }
        }
        return null;
    }

    private static String getLabelById(Collection<EPGMVertex> vertices, GradoopId id) {
        EPGMVertex vertex = getVertexById(vertices, id);
        return vertex != null ? vertex.getLabel() : null;
    }

    public static void printAllPaths(LogicalGraph graph, DataSet<EPGMVertex> sortedVertices) throws Exception {
        Collection<EPGMVertex> vertices = sortedVertices.collect();

        // Adjacency list, used to represent the structure of the graph. The key is the ID of the vertex,
        // the value is the list of IDs of all neighbouring vertices of that vertex
        Map<GradoopId, List<GradoopId>> adjList = new HashMap<>();
        Map<GradoopId, List<GradoopId>> invAdjList = new HashMap<>();

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

        // A map to count the vertices. The key is the label of the vertex and the value is the number of times the vertex appears in all paths
        Map<String, Integer> vertexCount = new HashMap<>();

        // Get all vertices that do not have an incoming edge, i.e. vertices that could be the start of the search
        List<EPGMVertex> startingVertices = vertices.stream()
                .filter(vertex -> {
                    List<GradoopId> ls = invAdjList.get(vertex.getId());
                    return ls == null || ls.isEmpty();
                })
                .collect(Collectors.toList());

        // Iterate over all possible starting vertices
        for (EPGMVertex vertex : startingVertices) {
            // Current path, for depth-first search
            List<GradoopId> initialPath = new ArrayList<>();
            // All paths, each element being a path from the current vertex
            List<List<GradoopId>> allPaths = new ArrayList<>();
            // Visited vertices, to avoid repeated visits to the same vertex during the search
            Set<GradoopId> visited = new HashSet<>();
            dfs(vertex.getId(), initialPath, adjList, allPaths, visited);
            System.out.println("Paths starting from vertex " + vertex.getId() + ":");
            for (List<GradoopId> path : allPaths) {
                System.out.println("Path:");
                for (GradoopId id : path) {
                    EPGMVertex currentVertex = getVertexById(vertices, id);
                    String label = currentVertex.getLabel();
                    vertexCount.put(label, vertexCount.getOrDefault(label, 0) + 1);
                    System.out.println(currentVertex);
                }
                System.out.println();
            }
        }

        System.out.println("Vertex counts:");
        for (Map.Entry<String, Integer> entry : vertexCount.entrySet()) {
            System.out.println("Vertex " + entry.getKey() + " appears " + entry.getValue() + " times.");
        }
    }

    // Set the frequency threshold for labels to be 10
    private static final int FREQUENCY_THRESHOLD = 10;

    // Define the method for printing all simple paths
    public static void printAllSimplePaths(LogicalGraph graph, List<EPGMVertex> vertices) throws Exception {

        // Create an adjacency list and an inverse adjacency list
        Map<GradoopId, List<GradoopId>> adjList = new HashMap<>();
        Map<GradoopId, List<GradoopId>> invAdjList = new HashMap<>();
        // Create a mapping of label frequencies
        Map<String, Integer> labelFrequencies = new HashMap<>();

        // Traverse all edges in the graph
        for (EPGMEdge edge : graph.getEdges().collect()) {
            // Get the source vertex and target vertex of the edge
            GradoopId sourceId = edge.getSourceId();
            GradoopId targetId = edge.getTargetId();
            // Add the corresponding edge in the adjacency list and the inverse adjacency list
            adjList.computeIfAbsent(sourceId, k -> new ArrayList<>()).add(targetId);
            invAdjList.computeIfAbsent(targetId, k -> new ArrayList<>()).add(sourceId);
        }

        // Get all vertices without incoming edges, i.e., vertices without predecessor nodes
        List<EPGMVertex> startingVertices = vertices.stream()
                .filter(vertex -> {
                    List<GradoopId> ls = invAdjList.get(vertex.getId());
                    return ls == null || ls.isEmpty();
                })
                .collect(Collectors.toList());

        // Traverse all the starting vertices
        for (EPGMVertex vertex : startingVertices) {
            // For each starting vertex, calculate all shortest paths starting from this vertex
            Map<GradoopId, List<GradoopId>> shortestPaths = computeShortestPaths(vertex.getId(), new ArrayList<>(vertices), adjList);

            // Traverse all shortest paths
            for (List<GradoopId> path : shortestPaths.values()) {
                // Traverse each vertex in the path
                for (GradoopId id : path) {
                    // Get the label of the vertex
                    String label = getLabelById(vertices, id);
                    // If the label is not null, then increase the frequency of this label in the label frequency mapping
                    if (label != null) {
                        labelFrequencies.put(label, labelFrequencies.getOrDefault(label, 0) + 1);
                    }
                }
            }
        }

        // Print each label and its frequency
        for (Map.Entry<String, Integer> entry : labelFrequencies.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
        System.out.println();

        // Get the frequent labels, i.e., labels whose frequency is greater than or equal to the threshold
        Set<String> frequentLabels = labelFrequencies.entrySet().stream()
                .filter(entry -> entry.getValue() >= FREQUENCY_THRESHOLD)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

        // Print the frequent labels
        System.out.println("Frequent labels:");
        frequentLabels.forEach(System.out::println);
        System.out.println();

        // Traverse all the starting vertices
        for (EPGMVertex vertex : startingVertices) {
            // For each starting vertex, calculate all shortest paths starting from this vertex
            Map<GradoopId, List<GradoopId>> shortestPaths = computeShortestPaths(vertex.getId(), new ArrayList<>(vertices), adjList);

            // Print the starting vertex
            System.out.println("Shortest paths from vertex " + vertex.getId() + ":");
            // Traverse all shortest paths
            for (Map.Entry<GradoopId, List<GradoopId>> entry : shortestPaths.entrySet()) {
                // Get each vertex in the path
                List<GradoopId> path = entry.getValue();
                // Check whether the path contains frequent labels
                if (path.stream().anyMatch(id -> {
                    String label = getLabelById(vertices, id);
                    return label != null && frequentLabels.contains(label);
                })) {
                    // If it contains frequent labels, then print this path
                    System.out.println("Path to vertex " + entry.getKey() + ":");
                    for (GradoopId id : path) {
                        System.out.println(getVertexById(vertices, id));
                    }
                    System.out.println();
                }
            }
        }
    }


    // Calculate the shortest path from the source vertex to all other vertices
    public static Map<GradoopId, List<GradoopId>> computeShortestPaths(GradoopId sourceId, List<EPGMVertex> sortedVertices, Map<GradoopId, List<GradoopId>> adjList) {
        Map<GradoopId, Integer> distances = new HashMap<>();
        Map<GradoopId, GradoopId> previousNodes = new HashMap<>();
        Map<GradoopId, List<GradoopId>> shortestPaths = new HashMap<>();

        // Initialising distances and precursor nodes
        for (EPGMVertex vertex : sortedVertices) {
            GradoopId id = vertex.getId();
            distances.put(id, id.equals(sourceId) ? 0 : Integer.MAX_VALUE);
        }

        // Update distance and predecessor nodes
        for (EPGMVertex vertex : sortedVertices) {
            GradoopId id = vertex.getId();
            if (adjList.containsKey(id)) {
                for (GradoopId neighbourId : adjList.get(id)) {
                    int altDistance = distances.get(id) + 1;
                    if (altDistance < distances.get(neighbourId)) {
                        distances.put(neighbourId, altDistance);
                        previousNodes.put(neighbourId, id);
                    }
                }
            }
        }

        // Construct the shortest path based on the predecessor node
        for (GradoopId id : distances.keySet()) {
            if (!distances.get(id).equals(Integer.MAX_VALUE)) {
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

    public static void printLabelFrequencies(Map<String, Integer> labelFrequencies) {
        System.out.println("Label frequencies:");
        for (Map.Entry<String, Integer> entry : labelFrequencies.entrySet()) {
            System.out.println("Label: " + entry.getKey() + ", Frequency: " + entry.getValue());
        }
    }

    public static void dfs(GradoopId currentId, List<GradoopId> currentPath, Map<GradoopId, List<GradoopId>> adjList, List<List<GradoopId>> allPaths, Set<GradoopId> visited) {
        currentPath.add(currentId);
        visited.add(currentId);

        // If the current vertex has no neighbouring vertices, or if all neighbouring vertices have been visited,
        // then the current path is a complete path and is added to allPaths
        if (!adjList.containsKey(currentId) || adjList.get(currentId).isEmpty()) {
            allPaths.add(new ArrayList<>(currentPath));
        } else {
            // For each neighbouring vertex, if it has not already been visited, start a depth-first search from it
            for (GradoopId id : adjList.get(currentId)) {
                if (!visited.contains(id)) {
                    dfs(id, currentPath, adjList, allPaths, visited);
                }
            }
        }

        // Remove the current vertex during backtracking
        currentPath.remove(currentId);
        visited.remove(currentId);
    }

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        LogicalGraph graph = TestData.loadTestData(env);

        graph.print();

        List<EPGMVertex> topologicalSort = TopologicalSort.topologicalSort(graph);
        topologicalSort.stream().forEach(System.out::println);

        printAllSimplePaths(graph, topologicalSort);

    }
}

