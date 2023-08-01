package com.ncl.gradoopgraph.utils;

import com.ncl.gradoopgraph.Beans.GeneralFrequentPath;
import com.ncl.gradoopgraph.loadData.TestData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author gyj
 * @title: GeneralFrequentGraph_v1
 * @projectName gradoop-graph
 * @description: TODO
 * @date 7/25/23 4:16 AM
 */
public class GeneralFrequentGraph_v1 {

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

    private static Map<Integer, List<GeneralFrequentPath>> frequentSubgraphs = new HashMap<>();

    public static Set<GeneralFrequentPath> miningFrequentSubgraph(LogicalGraph graph, int minSup) throws Exception {

        // 使用拓扑排序对图中的节点进行排序，确保得到一个有向无环图（DAG）的线性表示
        // Sort the nodes in the graph using topological ordering to ensure that a linear representation of a directed acyclic graph (DAG) is obtained
        List<EPGMVertex> topologicalSortVertices = TopologicalSort.topologicalSort(graph);
        List<EPGMEdge> edges = graph.getEdges().collect();

        // 查找图中所有的简单路径
        // Find all simple paths in the graph
        Map<String, Map<GradoopId, Integer>> simplePaths = GeneralFrequentGraph.findAllSimplePaths(graph, topologicalSortVertices);

        // 创建一个映射，将顶点ID映射到EPGMVertex对象
        // Create a map that maps vertex IDs to EPGMVertex objects
        Map<GradoopId, EPGMVertex> idToVertexMap = new HashMap<>();
        for (EPGMVertex vertex : topologicalSortVertices) {
            idToVertexMap.put(vertex.getId(), vertex);
        }

        // 创建一个映射，将标签映射到顶点的ID集合
        // Create a mapping that maps labels to a collection of vertex IDs
        Map<String, Set<GradoopId>> labelToVertices = new HashMap<>();
        // 创建一个映射，将顶点ID映射到路径和路径值的映射
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
        //初始化两点一线 的 一般路径 作为起始
        //Initialise the general path of two points and one line as a start.
        Set<List<EPGMEdge>> generateSimplePaths = generateSimplePaths(graph);

        while (!generateSimplePaths.isEmpty()) {
            // Step 2: Candidate generation 生成该路径的 两两标签 的候选频繁模式
            // Step 2: Candidate generation Generate a candidate frequent pattern of two-by-two labels for the path.
            List<GeneralFrequentPath> candidates = generateF(generateSimplePaths, idToVertexMap, vertexToPaths);

            System.out.println("k = " + k);
            candidates.stream().forEach(System.out::println);

            List<GeneralFrequentPath> nextFrequentSubgraphs = new ArrayList<>();
            for (GeneralFrequentPath candidate : candidates) {
                // Step 3: Isomorphism checkin
                if (!checkIsomorphism(frequentSubgraphs, candidate)) {
                    // Step 4: Support counting
                    int support = countSupport(candidate, labelToVertices, vertexToPaths);
                    // Step 5: Check if support > minsup
                    if (support > minSup) {
                        nextFrequentSubgraphs.add(candidate);
                    }
                }
            }
            frequentSubgraphs.put(k, nextFrequentSubgraphs);
            k += 1;
            generateSimplePaths = generateSimplePaths(generateSimplePaths, edges);
        }

        return mergeFrequentSubgraphs(frequentSubgraphs);
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

    private static List<GeneralFrequentPath> generateF(Set<List<EPGMEdge>> simplePaths,
                                                       Map<GradoopId, EPGMVertex> idToVertexMap,
                                                       Map<GradoopId, Map<String, Integer>> vertexToPaths) throws Exception {

        List<GeneralFrequentPath> generalPaths = new ArrayList<>();

        for (List<EPGMEdge> path : simplePaths) {
            // 每个路径的第一条边的源顶点作为起始顶点，最后一条边的目标顶点作为结束顶点
            // The source vertex of the first edge of each path is the start vertex and the target vertex of the last edge is the end vertex.
            EPGMEdge firstEdge = path.get(0);
            EPGMEdge lastEdge = path.get(path.size() - 1);

            Set<Integer> pathIds = new HashSet<>();

            Map<String, Integer> paths = vertexToPaths.get(firstEdge.getSourceId());
            Map<String, Integer> pathe = vertexToPaths.get(lastEdge.getTargetId());

            Set<String> intersection = new HashSet<>(paths.keySet());
            intersection.retainAll(pathe.keySet());

            for (String pathKey : intersection) {
                if (paths.get(pathKey) < pathe.get(pathKey)) {
                    // Get the path ID and add it to the list
                    pathIds.add(Integer.parseInt(pathKey.substring(4))); // 获取路径ID并添加到列表中
                }
            }

            String startLabel = idToVertexMap.get(firstEdge.getSourceId()).getLabel();
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

    private static boolean checkIsomorphism(Map<Integer, List<GeneralFrequentPath>> frequentSubgraphs, GeneralFrequentPath candidate) {
        for (List<GeneralFrequentPath> paths : frequentSubgraphs.values()) {
            for (GeneralFrequentPath path : paths) {
                if (candidate.equals(path) & candidate.containsEdgeIdsOf(path)) {
                    return true;
                }
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

        // 创建一个映射来存储 startNodeLabel 和 endNodeLabel 的组合到 GeneralFrequentPath 的映射
        // Create a map to store the combination of startNodeLabel and endNodeLabel to the GeneralFrequentPath.
        Map<String, GeneralFrequentPath> mergeMap = new HashMap<>();

        // 遍历每个 GeneralFrequentPath，如果具有相同的 startNodeLabel 和 endNodeLabel，则合并其 edgeIds
        // Iterate over each GeneralFrequentPath and merge the edgeIds if they have the same startNodeLabel and endNodeLabel.
        for (List<GeneralFrequentPath> paths : frequentSubgraphs.values()) {
            for (GeneralFrequentPath path : paths) {
                String key = path.getStartNodeLabel() + "-" + path.getEndNodeLabel();
                GeneralFrequentPath existingPath = mergeMap.get(key);

                if (existingPath != null) {
                    // 如果已经存在具有相同 startNodeLabel 和 endNodeLabel 的路径，则合并 edgeIds
                    // If there are already paths with the same startNodeLabel and endNodeLabel, merge edgeIds
                    existingPath.getEdgeIds().addAll(path.getEdgeIds());
                } else {
                    // 否则，添加新路径到映射
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

        Set<GeneralFrequentPath> frequentPaths = miningFrequentSubgraph(graph, 3);
        System.out.println();
        System.out.println(" ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ ");

        frequentPaths.stream().forEach(System.out::println);
    }

}