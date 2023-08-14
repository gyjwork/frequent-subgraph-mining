package com.ncl.gradoopgraph.utils;

import com.ncl.gradoopgraph.Beans.GeneralFrequentPath;
import com.ncl.gradoopgraph.loadData.TestData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author gyj
 * @title: GeneralFrequentGraph_v2
 * @projectName gradoop-graph
 * @description: TODO
 * @date 8/2/236:49 PM
 */
public class GeneralFrequentGraph_v2 {

    /**
     * // G is the database
     * // k is initialize to 1
     * Mining Frequent Subgraph(G, minsup):
     * 0.   Populate F1
     * 1.   while Fk not =  0
     * 2.    Ck+1 = Candidate generation(Fk, G)
     * 3.    forall c € Ck+1
     * 4.       if isomorphism checking(c) = true
     * 5.          support counting(c, G)]
     * 6.          if c.sup > minsup
     * 7.      Fk+1 = Fk+1 U{c}
     * 8.    k = k +1
     * 9.  return Ui=1…k-1Fi
     */

    private static Map<Integer, LogicalGraph> frequentSubGraphs = new HashMap<>();
    private static Map<String, GeneralFrequentPath> nextFrequentSubgraphs = new HashMap<>();

    public static Map<Integer, LogicalGraph> miningFrequentSubGraph(LogicalGraph graph, int minSup) throws Exception {

        // Sort the nodes in the graph using topological ordering to ensure that a linear representation of a directed acyclic graph (DAG) is obtained
        List<EPGMVertex> topologicalSortVertices = TopologicalSort.topologicalSort(graph);
        List<EPGMEdge> edges = graph.getEdges().collect();

        // Find all simple paths in the graph
        Map<String, Map<GradoopId, Integer>> simplePaths = GeneralFrequentGraph.findAllSimplePaths(graph, topologicalSortVertices);

        // Create a map that maps vertex IDs to EPGMVertex objects
        Map<GradoopId, EPGMVertex> idToVertexMap = new HashMap<>();
        for (EPGMVertex vertex : topologicalSortVertices) {
            idToVertexMap.put(vertex.getId(), vertex);
        }

        // Create a mapping that maps labels to a collection of vertex IDs
        Map<String, Set<GradoopId>> labelToVertices = new HashMap<>();
        // Create a mapping that maps vertex IDs to paths and path values
        Map<GradoopId, Map<String, Integer>> vertexToPaths = new HashMap<>();

        for (Map.Entry<String, Map<GradoopId, Integer>> entry : simplePaths.entrySet()) {
            Map<GradoopId, Integer> path = entry.getValue();

            for (GradoopId id : path.keySet()) {
                EPGMVertex vertex = idToVertexMap.get(id);
                Integer pathValue = path.get(id);

                labelToVertices.computeIfAbsent(vertex.getLabel(), k -> new HashSet<>()).add(id);
                vertexToPaths.computeIfAbsent(id, k -> new HashMap<>()).put(entry.getKey(), pathValue);
            }
        }

        // Step 1: Iterate until Fk is empty
        int k = 1;
        //Initialise the general path of two points and one line as a start.
        Set<List<EPGMEdge>> generateSimplePaths = generateSimplePaths(graph);

        while (!generateSimplePaths.isEmpty()) {

            //Map<String, GeneralFrequentPath> nextFrequentSubgraphs = new HashMap<>();

            for (List<EPGMEdge> candidateEdges : generateSimplePaths) {
                // Step 2: Generate candidate frequent subGraphs (original graph labelled as points)
                List<GeneralFrequentPath> candidates = generateF(candidateEdges, idToVertexMap, vertexToPaths);

                System.out.println("k = " + k);
                candidates.stream().forEach(System.out::println);

                for (GeneralFrequentPath candidate : candidates) {
                    // Step 3: Isomorphism checking
                    String key = candidate.getStartNodeLabel().concat("-").concat(candidate.getEndNodeLabel());
                    if (!checkIsomorphism(nextFrequentSubgraphs, candidate)) {
                        // Step 4: Support counting
                        int support = countSupport(candidate, labelToVertices, vertexToPaths);
                        // Step 5: Check if support > minsup
                        if (support > minSup) {
                            nextFrequentSubgraphs.merge(key, candidate, (existing, newCandidate) -> existing.merge(newCandidate));
                        }
                    }
                }
            }
            if (!nextFrequentSubgraphs.isEmpty()) {
                HashSet<GeneralFrequentPath> paths = new HashSet<>(nextFrequentSubgraphs.values());
                LogicalGraph logicalGraph = pathGraphGenerator(paths, graph.getConfig());
                frequentSubGraphs.put(k, logicalGraph);
            }

            k += 1;
            generateSimplePaths = generateSimplePaths(generateSimplePaths, edges);

        }

        return frequentSubGraphs;
    }

    private static LogicalGraph pathGraphGenerator(HashSet<GeneralFrequentPath> paths, GradoopFlinkConfig config) {

        ArrayList<EPGMVertex> vertices = new ArrayList<>();
        ArrayList<EPGMEdge> edges = new ArrayList<>();

        HashMap<String, EPGMVertex> labelToVertex = new HashMap<>();

        for (GeneralFrequentPath path : paths) {
            // Get or create the start vertex
            EPGMVertex startVertex = labelToVertex.get(path.getStartNodeLabel());
            if (startVertex == null) {
                Properties properties = new Properties();
                properties.set("name", path.getStartNodeLabel());

                startVertex = new EPGMVertex();
                startVertex.setId(GradoopId.get());
                startVertex.setLabel("frequentNode");
                startVertex.setProperties(properties);

                vertices.add(startVertex);
                labelToVertex.put(path.getStartNodeLabel(), startVertex);
            }

            // Get or create the end vertex
            EPGMVertex endVertex = labelToVertex.get(path.getEndNodeLabel());
            if (endVertex == null) {
                org.gradoop.common.model.impl.properties.Properties properties = new org.gradoop.common.model.impl.properties.Properties();
                properties.set("name", path.getEndNodeLabel());

                endVertex = new EPGMVertex();
                endVertex.setId(GradoopId.get());
                endVertex.setLabel("frequentNode");
                endVertex.setProperties(properties);

                vertices.add(endVertex);
                labelToVertex.put(path.getEndNodeLabel(), endVertex);
            }

            // Create a new edge
            Properties edgeProperties = new Properties();
            edgeProperties.set("edgeIdList", path.getEdgeIds().toString());
            EPGMEdge edge = new EPGMEdge(GradoopId.get(), "frequentEdge", startVertex.getId(), endVertex.getId(), edgeProperties, new GradoopIdSet());
            edges.add(edge);
        }

        DataSet<EPGMVertex> verticesDataSet = config.getExecutionEnvironment().fromCollection(vertices);
        DataSet<EPGMEdge> edgesDataSet = config.getExecutionEnvironment().fromCollection(edges);

        return config.getLogicalGraphFactory().fromDataSets(verticesDataSet, edgesDataSet);
    }

    private static Set<List<EPGMEdge>> generateSimplePaths(Set<List<EPGMEdge>> existingPaths, Collection<EPGMEdge> edges) throws Exception {

        // Group edges by source ID for efficient lookup
        Map<GradoopId, List<EPGMEdge>> edgesBySource = new HashMap<>();
        for (EPGMEdge edge : edges) {
            edgesBySource.computeIfAbsent(edge.getSourceId(), k -> new ArrayList<>()).add(edge);
        }

        // Start with the existing paths
        List<List<EPGMEdge>> paths = new ArrayList<>(existingPaths);

        System.out.println("Initial paths:");
        printPaths(paths);

        // Build longer paths
        List<List<EPGMEdge>> newPaths = new ArrayList<>();
        for (List<EPGMEdge> path : paths) {
            GradoopId lastTargetId = path.get(path.size() - 1).getTargetId();
            List<EPGMEdge> extendingEdges = edgesBySource.getOrDefault(lastTargetId, Collections.emptyList());
            for (EPGMEdge nextEdge : extendingEdges) {
                List<EPGMEdge> newPath = new ArrayList<>(path);
                newPath.add(nextEdge);
                newPaths.add(newPath);
            }
        }

        System.out.println("newPaths after extension:");
        printPaths(newPaths);

        // Convert result into a Set
        return new HashSet<>(newPaths);
    }

    private static void printPaths(List<List<EPGMEdge>> paths) {
        for (List<EPGMEdge> path : paths) {
            System.out.print("Path: ");
            for (EPGMEdge edge : path) {
                System.out.print(edge.getSourceId() + "-" + edge.getTargetId() + " ");
            }
            System.out.println();
        }
    }

    private static Set<List<EPGMEdge>> generateSimplePaths(LogicalGraph graph) throws Exception {

        DataSet<EPGMEdge> edgeDataSet = graph.getEdges();

        // Transform each edge into a list containing that single edge
        DataSet<List<EPGMEdge>> edgesAsLists = edgeDataSet.map(new MapFunction<EPGMEdge, List<EPGMEdge>>() {
            @Override
            public List<EPGMEdge> map(EPGMEdge edge) throws Exception {
                return Collections.singletonList(edge);
            }
        });

        // Collect the result and put it into a Set
        Set<List<EPGMEdge>> edgeSet = new HashSet<>(edgesAsLists.collect());

        return edgeSet;
    }

    private static List<GeneralFrequentPath> generateF(List<EPGMEdge> simplePath,
                                                       Map<GradoopId, EPGMVertex> idToVertexMap,
                                                       Map<GradoopId, Map<String, Integer>> vertexToPaths) throws Exception {

        List<GeneralFrequentPath> generalPaths = new ArrayList<>();

        // Get the newly added edge
        EPGMEdge lastEdge = simplePath.get(simplePath.size() - 1);

        for (int i = 0; i < simplePath.size(); i++) {
            EPGMEdge currentEdge = simplePath.get(i);

            Set<Integer> pathIds = new HashSet<>();

            Map<String, Integer> pathsStart = vertexToPaths.get(currentEdge.getSourceId());
            Map<String, Integer> pathsEnd = vertexToPaths.get(lastEdge.getTargetId());

            Set<String> intersection = new HashSet<>(pathsStart.keySet());
            intersection.retainAll(pathsEnd.keySet());

            for (String pathKey : intersection) {
                if (pathsStart.get(pathKey) < pathsEnd.get(pathKey)) {
                    // Get the path ID and add it to the list
                    pathIds.add(Integer.parseInt(pathKey.substring(4)));
                }
            }

            String startLabel = idToVertexMap.get(currentEdge.getSourceId()).getLabel();
            String endLabel = idToVertexMap.get(lastEdge.getTargetId()).getLabel();

            generalPaths.add(new GeneralFrequentPath(startLabel, pathIds, endLabel));
        }

        return mergePaths(generalPaths);
    }

    public static List<GeneralFrequentPath> mergePaths(List<GeneralFrequentPath> paths) {
        return new ArrayList<>(paths.stream().collect(
                Collectors.toMap(
                        path -> Arrays.asList(path.getStartNodeLabel(), path.getEndNodeLabel()), // key
                        Function.identity(), // value
                        GeneralFrequentPath::merge) // merge function
        ).values());
    }

    private static boolean checkIsomorphism(Map<String, GeneralFrequentPath> frequentPatterns, GeneralFrequentPath candidate) {

        for (GeneralFrequentPath path : frequentPatterns.values()) {
            if (path.equals(candidate) && path.containsEdgeIdsOf(candidate)) {
                return true;
            }
        }
        return false;
    }

    private static int countSupport(GeneralFrequentPath candidate,
                                    Map<String, Set<GradoopId>> labelToVertices,
                                    Map<GradoopId, Map<String, Integer>> vertexToPaths) {

        // Get the start and end labels of candidate paths
        String startLabel = candidate.getStartNodeLabel();
        String endLabel = candidate.getEndNodeLabel();

        // Create a Set to avoid double-counted paths
        Set<String> countedPaths = new HashSet<>();

        Set<GradoopId> startVertices = labelToVertices.get(startLabel);
        Set<GradoopId> endVertices = labelToVertices.get(endLabel);

        // Create a variable to count the number of paths that satisfy the condition
        int totalCount = 0;

        for (GradoopId ids : startVertices) {
            Map<String, Integer> paths = vertexToPaths.get(ids);
            for (GradoopId ide : endVertices) {
                Map<String, Integer> pathe = vertexToPaths.get(ide);
                Set<String> intersection = new HashSet<>(paths.keySet());
                intersection.retainAll(pathe.keySet());
                for (String path : intersection) {
                    if (paths.get(path) < pathe.get(path) && !countedPaths.contains(path)) {
                        countedPaths.add(path);
                        totalCount++;
                    }
                }
            }
        }

        return totalCount;
    }

    private static Set<GeneralFrequentPath> mergeFrequentSubgraphs(Map<Integer, List<GeneralFrequentPath>> frequentSubgraphs) {

        System.out.println();
        System.out.println(" +++++++++++++++++++++++++++++frequentSubgraphs+++++++++++++++++++++++++++++ ");

        frequentSubgraphs.forEach((key, value) -> {
            System.out.println("Key: " + key);
            value.forEach(path -> System.out.println("    " + path));
        });

        // Create a map to store the combination of startNodeLabel and endNodeLabel to the GeneralFrequentPath.
        Map<String, GeneralFrequentPath> mergeMap = new HashMap<>();

        // Iterate over each GeneralFrequentPath and merge the edgeIds if they have the same startNodeLabel and endNodeLabel.
        for (List<GeneralFrequentPath> paths : frequentSubgraphs.values()) {
            for (GeneralFrequentPath path : paths) {
                String key = path.getStartNodeLabel() + "-" + path.getEndNodeLabel();
                GeneralFrequentPath existingPath = mergeMap.get(key);

                if (existingPath != null) {
                    // If there are already paths with the same startNodeLabel and endNodeLabel, merge edgeIds
                    existingPath.getEdgeIds().addAll(path.getEdgeIds());
                } else {
                    // Otherwise, add a new path to the map
                    mergeMap.put(key, path);
                }
            }
        }

        return new HashSet<>(mergeMap.values());
    }

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        LogicalGraph graph = TestData.loadTestData(env);
        graph.print();

        Map<Integer, LogicalGraph> frequentSubGraph = miningFrequentSubGraph(graph, 3);
        System.out.println();
        System.out.println(" ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ ");

        for (Map.Entry<Integer, LogicalGraph> entry : frequentSubGraph.entrySet()) {
            System.out.println("Key: " + entry.getKey());
            entry.getValue().print();
        }
    }

}

