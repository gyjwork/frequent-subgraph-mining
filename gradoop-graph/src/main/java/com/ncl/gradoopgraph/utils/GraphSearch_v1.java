package com.ncl.gradoopgraph.utils;

import com.ncl.gradoopgraph.Beans.SimplePath;
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
 * @title: GraphSearch_v1
 * @projectName gradoop-graph
 * @description: TODO
 * @date 7/12/233:49 PM
 */
public class GraphSearch_v1 {

    // Auxiliary method for getting the vertices with a given ID from the dataset
    private static EPGMVertex getVertexById(Collection<EPGMVertex> vertices, GradoopId id) {
        for (EPGMVertex vertex : vertices) {
            if (vertex.getId().equals(id)) {
                return vertex;
            }
        }
        return null;
    }


    public static void findAllSimplePaths(LogicalGraph graph, List<EPGMVertex> vertices) throws Exception {

        // Used to store forward and reverse adjacency tables
        Map<GradoopId, List<GradoopId>> adjList = new HashMap<>();
        Map<GradoopId, List<GradoopId>> invAdjList = new HashMap<>();

        // Used to record the next available path id
        int[] pathIdCounter = new int[1];

        // Iterate over all edges in the graph and construct forward and reverse neighbourhood tables
        for (EPGMEdge edge : graph.getEdges().collect()) {
            GradoopId sourceId = edge.getSourceId();
            GradoopId targetId = edge.getTargetId();
            adjList.computeIfAbsent(sourceId, k -> new ArrayList<>()).add(targetId);
            invAdjList.computeIfAbsent(targetId, k -> new ArrayList<>()).add(sourceId);
        }

        // Find all the nodes that are not in the edges, these nodes will be used as the starting nodes of the path
        List<EPGMVertex> startingVertices = vertices.stream()
                .filter(vertex -> {
                    List<GradoopId> ls = invAdjList.get(vertex.getId());
                    return ls == null || ls.isEmpty();
                })
                .collect(Collectors.toList());

        // Used to store the set of vertices corresponding to each label, and the set of paths corresponding to each vertex
        Map<String, Set<GradoopId>> labelToVertices = new HashMap<>();
        Map<GradoopId, Map<String, Integer>> vertexToPaths = new HashMap<>();

        // Find all the nodes that are not out of the edge, these nodes will be used as the terminating nodes of the path
        Set<GradoopId> terminalVertices = vertices.stream()
                .filter(vertex -> {
                    List<GradoopId> ls = adjList.get(vertex.getId());
                    return ls == null || ls.isEmpty();
                })
                .map(EPGMVertex::getId)
                .collect(Collectors.toSet());

        // Iterate over all the start nodes and call the computeShortestPaths method on each node to compute all the shortest paths from that node.
        Map<String, Map<GradoopId, Integer>> shortestPathss = new HashMap<>();

        for (EPGMVertex vertex : startingVertices) {
            GradoopId sourceId = vertex.getId();
            Map<String, Map<GradoopId, Integer>> shortestPaths = computeShortestPaths(sourceId, new ArrayList<>(vertices), adjList, terminalVertices, pathIdCounter);
            shortestPathss.putAll(shortestPaths);
            System.out.println("Shortest paths from vertex " + sourceId + ":");

            // Print out all the shortest paths from the current node
            for (Map.Entry<String, Map<GradoopId, Integer>> entry : shortestPaths.entrySet()) {
                System.out.println("Path id " + entry.getKey() + " to vertex " + entry.getValue().keySet().stream().max(Comparator.comparing(entry.getValue()::get)).get() + ":");
                for (GradoopId id : entry.getValue().keySet()) {
                    System.out.println(getVertexById(vertices, id));
                    labelToVertices.computeIfAbsent(getVertexById(vertices, id).getLabel(), k -> new HashSet<>()).add(id);
                    vertexToPaths.computeIfAbsent(id, k -> new HashMap<>()).put(entry.getKey(), entry.getValue().get(id));
                }
                System.out.println();
            }
        }
        // Print out the mapping of labels to vertex collections and vertices to path collections
        System.out.println("Label to Vertices mapping: " + labelToVertices);
        System.out.println();
        System.out.println("Vertex to Paths mapping: " + vertexToPaths);

        // Find all paths from "buys" to "sells" that have at least 2 occurrences in the entire dataset.
        findPaths("buys", "transfers_ownership", labelToVertices, vertexToPaths, 2);
    }


    /*
      This method is designed to compute the shortest paths from a given source node to all other nodes. The method uses Dijkstra's algorithm.
      In Dijkstra's algorithm, we start at the source node and "visit" all the nodes step by step, and at each step we are guaranteed that we have found the shortest path to the nodes visited so far.
      The shortest path from the source node to each terminal node is returned as a map, where the key is a string consisting of path ID and path counter, and the value is a map whose key is the node ID and value is the node in the path s position.
     */
    public static Map<String, Map<GradoopId, Integer>> computeShortestPaths(GradoopId sourceId, List<EPGMVertex> sortedVertices,
                                                                            Map<GradoopId, List<GradoopId>> adjList,
                                                                            Set<GradoopId> terminalVertices, int[] pathIdCounter) {
        // Initialise a mapping to store the shortest distance from each node to the source node
        Map<GradoopId, Integer> distances = new HashMap<>();

        // Initialise a map to store the previous node in the shortest path for each node
        Map<GradoopId, GradoopId> previousNodes = new HashMap<>();

        // Initialise a map to store the shortest paths found
        Map<String, Map<GradoopId, Integer>> shortestPaths = new HashMap<>();

        // Initialise a queue to store the nodes to be accessed
        Queue<GradoopId> toVisit = new LinkedList<>();

        // For all nodes, initialise their distance to the source node and the precursor node
        for (EPGMVertex vertex : sortedVertices) {
            GradoopId id = vertex.getId();
            // If it is a source node, the distance is set to 0 and added to the pending access queue
            if (id.equals(sourceId)) {
                distances.put(id, 0);
                toVisit.add(id);
            } else {
                // For other nodes, the distance is initialised to infinity
                distances.put(id, Integer.MAX_VALUE);
            }
            // Initialise the predecessor node to null
            previousNodes.put(id, null);
        }

        // When the queue to be accessed is not empty, continue the execution of the loop
        while (!toVisit.isEmpty()) {
            // Take the first node of the queue to be accessed
            GradoopId id = toVisit.remove();

            // If the node has neighbouring nodes
            if (adjList.containsKey(id)) {
                // Iterate over neighbouring nodes
                for (GradoopId neighbourId : adjList.get(id)) {
                    // Calculate new distance (current node's distance plus 1)
                    int altDistance = distances.get(id) + 1;

                    // If the new distance is less than the current distance of the neighbouring nodes
                    if (altDistance < distances.get(neighbourId)) {
                        // Update the distance and antecedent nodes of the neighbouring nodes and add the neighbouring nodes to the pending access queue
                        distances.put(neighbourId, altDistance);
                        previousNodes.put(neighbourId, id);
                        toVisit.add(neighbourId);
                    }
                }
            }
        }

        // For all nodes, if it is a terminating node
        for (GradoopId id : distances.keySet()) {
            if (terminalVertices.contains(id)) {
                // Initialize a list to store the shortest path from the source node to the terminal node
                List<GradoopId> path = new ArrayList<>();

                // Initialize a map for storing the position of each node in the path
                Map<GradoopId, Integer> nodePositions = new HashMap<>();
                GradoopId currentNode = id;
                int position = 0;

                // Find the shortest path by backtracking the predecessor node
                while (currentNode != null) {
                    path.add(0, currentNode);
                    nodePositions.put(currentNode, position++);
                    currentNode = previousNodes.get(currentNode);
                }

                // Add the shortest path found to the shortest path map
                shortestPaths.put("path" + pathIdCounter[0]++, nodePositions);

                // Adjust the position of each node in the path so that the position of the source node is 0 and the position of the termination node is the length of the path minus one
                for (GradoopId nodeId : nodePositions.keySet()) {
                    nodePositions.put(nodeId, path.size() - nodePositions.get(nodeId) - 1);
                }
            }
        }

        return shortestPaths;
    }

    /*

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

        // Used to store general paths that satisfy conditions
        HashSet<SimplePath> generalPaths = new HashSet<>();

        // Paths for avoiding double counting
        Set<String> countedPaths = new HashSet<>();

        // Get the nodes under the start and end labels
        Set<GradoopId> startVertices = labelToVertices.get(startLabel);
        Set<GradoopId> endVertices = labelToVertices.get(endLabel);

        int count = 0;

        // For each pair of start and end nodes, find all the paths where they are on the same path and the start node is in front of the end node
        for (GradoopId ids : startVertices) {
            Map<String, Integer> paths = vertexToPaths.get(ids);
            for (GradoopId ide : endVertices) {
                Map<String, Integer> pathe = vertexToPaths.get(ide);
                Set<String> intersection = new HashSet<>(paths.keySet());
                // Get the intersection of two sets
                intersection.retainAll(pathe.keySet());
                for (String path : intersection) {
                    // If the position of the start node in the path is less than the position of the end node, and the path has not been counted yet,
                    // then increase the value of the counter and add the path to the set of counted paths
                    if (paths.get(path) < pathe.get(path) && !countedPaths.contains(path)) {
                        count++;
                        countedPaths.add(path);
                    }
                }
                // Print out the number of shared paths for both nodes
                System.out.println("Number of common elements: " + intersection.size());
            }
        }

        // If the number of paths that satisfy the condition is greater than or equal to the minimum support,
        // then add this path to the result set
        if (count >= minSupport) {
            generalPaths.add(new SimplePath(startLabel, endLabel));
        }

        // Print out the number and labels of paths that satisfy the condition
        System.out.println(new SimplePath(startLabel, endLabel).toString() + " number : " + count);

        generalPaths.forEach(System.out::println);

        return generalPaths;

    }

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        LogicalGraph graph = TestData.loadTestData(env);

        graph.print();

        List<EPGMVertex> topologicalSort = TopologicalSort.topologicalSort(graph);
        topologicalSort.stream().forEach(System.out::println);

        findAllSimplePaths(graph, topologicalSort);
    }
}
