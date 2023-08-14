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

        // Find all the nodes that are not out of the edge, these nodes will be used as the terminating nodes of the path
        Set<GradoopId> terminalVertices = vertices.stream()
                .filter(vertex -> {
                    List<GradoopId> ls = adjList.get(vertex.getId());
                    return ls == null || ls.isEmpty();
                })
                .map(EPGMVertex::getId)
                .collect(Collectors.toSet());

        // Iterate over all the start nodes and call the computeShortestPaths method on each node to compute all the shortest paths from that node.
        Map<String, Map<GradoopId, Integer>> shortestPaths = new HashMap<>();
        for (EPGMVertex vertex : startingVertices) {
            GradoopId sourceId = vertex.getId();
            Map<String, Map<GradoopId, Integer>> shortestPath = computeShortestPaths(sourceId, new ArrayList<>(vertices), adjList, terminalVertices, pathIdCounter);
            shortestPaths.putAll(shortestPath);
            //System.out.println("Shortest paths from vertex " + sourceId + ":");

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
      This method is designed to compute the shortest paths from a given source node to all other nodes. The method uses Dijkstra's algorithm.
      In Dijkstra's algorithm, we start at the source node and "visit" all the nodes step by step, and at each step we are guaranteed that we have found the shortest path to the nodes visited so far.
      The shortest path from the source node to each terminal node is returned as a map, where the key is a string consisting of path ID and path counter, and the value is a map whose key is the node ID and value is the node in the path s position.
     */
    private static Map<String, Map<GradoopId, Integer>> computeShortestPaths(GradoopId sourceId, List<EPGMVertex> sortedVertices,
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
}
