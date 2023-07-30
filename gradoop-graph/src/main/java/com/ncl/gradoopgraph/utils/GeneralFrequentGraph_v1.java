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
import org.gradoop.flink.util.GradoopFlinkConfig;

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

    // Fk maps to a collection of subgraphs (each represented as a list of edges)
    private static Map<Integer, List<GeneralFrequentPath>> frequentSubgraphs = new HashMap<>();

    public static Set<GeneralFrequentPath> miningFrequentSubgraph(LogicalGraph graph, ExecutionEnvironment env, int minSup) throws Exception {
        GradoopFlinkConfig config = graph.getConfig();

        // 1. Perform topological sort on the graph
        DataSet<EPGMVertex> topologicalSort = TopologicalSort.topologicalSort(graph, env);
        Collection<EPGMVertex> vertices = topologicalSort.collect();

        // 2. Find all simple paths
        Map<String, Map<GradoopId, Integer>> simplePaths = GeneralFrequentGraph.findAllSimplePaths(graph, topologicalSort);

        Map<String, Set<GradoopId>> labelToVertices = new HashMap<>();
        Map<GradoopId, Map<String, Integer>> vertexToPaths = new HashMap<>();

        for (Map.Entry<String, Map<GradoopId, Integer>> entry : simplePaths.entrySet()) {
            Map<GradoopId, Integer> path = entry.getValue();

            for (GradoopId id : path.keySet()) {
                EPGMVertex vertex = GeneralFrequentGraph.getVertexById(vertices, id);
                Integer pathValue = path.get(id);

                labelToVertices.computeIfAbsent(vertex.getLabel(), k -> new HashSet<>()).add(id);
                vertexToPaths.computeIfAbsent(id, k -> new HashMap<>()).put(entry.getKey(), pathValue);
            }
        }

        // Step 0: Populate F1
        //frequentSubgraphs.put(1, generateF1(graph, vertexToPaths));

        // Step 1: Iterate until Fk is empty
        int k = 0;
        Set<List<EPGMEdge>> generateSimplePaths = generateSimplePaths(graph, k + 1 );

        while (!generateSimplePaths.isEmpty()) {
            // Step 2: Candidate generation
            List<GeneralFrequentPath> candidates = generateF1(graph, vertexToPaths);

            List<GeneralFrequentPath> nextFrequentSubgraphs = new ArrayList<>();
            for (GeneralFrequentPath candidate : candidates) {
                // Step 4: Isomorphism checkin
                if (!checkIsomorphism(frequentSubgraphs, candidate)) {
                    // Step 5: Support counting
                    int support = countSupport(candidate, labelToVertices, vertexToPaths);
                    // Step 6: Check if support > minsup
                    if (support > minSup) {
                        nextFrequentSubgraphs.add(candidate);
                    }
                }
            }
            frequentSubgraphs.put(k + 1, nextFrequentSubgraphs);
            k += 1;
            generateSimplePaths = generateSimplePaths(graph, k + 1 );
        }

        // Create the resulting graph (collection of graphs)
        return mergeFrequentSubgraphs(frequentSubgraphs);
    }

    private static Set<List<EPGMEdge>> generateSimplePaths(LogicalGraph graph, int length) throws Exception {

        length += 1;

        DataSet<EPGMEdge> edgeDataSet = graph.getEdges();

        // Group edges by source ID for efficient lookup
        Map<GradoopId, List<EPGMEdge>> edgesBySource = new HashMap<>();
        for (EPGMEdge edge : edgeDataSet.collect()) {
            edgesBySource.computeIfAbsent(edge.getSourceId(), k -> new ArrayList<>()).add(edge);
        }

        // Transform each edge into a list containing that single edge
        List<List<EPGMEdge>> paths = edgeDataSet.collect().stream()
                .map(edge -> Collections.singletonList(edge))
                .collect(Collectors.toList());

        // Build longer paths
        for (int i = 1; i < length; i++) {
            List<List<EPGMEdge>> newPaths = new ArrayList<>();
            for (List<EPGMEdge> path : paths) {
                GradoopId targetId = path.get(path.size() - 1).getTargetId();
                List<EPGMEdge> nextEdges = edgesBySource.getOrDefault(targetId, Collections.emptyList());
                for (EPGMEdge nextEdge : nextEdges) {
                    List<EPGMEdge> newPath = new ArrayList<>(path);
                    newPath.add(nextEdge);
                    newPaths.add(newPath);
                }
            }
            paths = newPaths;
        }

        // Convert result into a Set
        return new HashSet<>(paths);
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

    private static List<GeneralFrequentPath> generateF1(LogicalGraph graph, Map<GradoopId, Map<String, Integer>> vertexToPaths) throws Exception {
        DataSet<EPGMEdge> edgeDataSet = graph.getEdges();
        Collection<EPGMVertex> vertices = graph.getVertices().collect();

        DataSet<GeneralFrequentPath> frequentPaths = edgeDataSet.map(new MapFunction<EPGMEdge, GeneralFrequentPath>() {
            @Override
            public GeneralFrequentPath map(EPGMEdge edge) throws Exception {
                Set<Integer> pathIds = new HashSet<>();

                Map<String, Integer> paths = vertexToPaths.get(edge.getSourceId());
                Map<String, Integer> pathe = vertexToPaths.get(edge.getTargetId());

                Set<String> intersection = new HashSet<>(paths.keySet());
                intersection.retainAll(pathe.keySet());

                for (String path : intersection) {
                    if (paths.get(path) < pathe.get(path)) {
                        pathIds.add(Integer.parseInt(path.substring(4))); // 获取路径ID并添加到列表中
                    }
                }

                String startLabel = GeneralFrequentGraph.getVertexById(vertices, edge.getSourceId()).getLabel();
                String endLabel = GeneralFrequentGraph.getVertexById(vertices, edge.getTargetId()).getLabel();

                return new GeneralFrequentPath(startLabel, pathIds, endLabel);
            }
        });

        /**
         *  合并GeneralFrequentPath， 如 A[1,2]B 和 A[1,3]B 合并为 A[1,2，3]B
         */
        return mergePaths(frequentPaths.collect());
    }

    public static List<GeneralFrequentPath> mergePaths(List<GeneralFrequentPath> paths) {
        return new ArrayList<>(paths.stream().collect(
                Collectors.toMap(
                        path -> Arrays.asList(path.getStartNodeLabel(), path.getEndNodeLabel()), // key
                        Function.identity(), // value
                        GeneralFrequentPath::merge) // merge function
        ).values());
    }


    private static Set<List<EPGMEdge>> generateCandidates(Set<List<EPGMEdge>> frequentSubgraphs, LogicalGraph graph) throws Exception {
        Set<List<EPGMEdge>> candidates = new HashSet<>();

        DataSet<EPGMEdge> allEdges = graph.getEdges();

        // 遍历每一个频繁子图
        for (List<EPGMEdge> frequentSubgraph : frequentSubgraphs) {
            // 遍历频繁子图中的每一条边
            for (EPGMEdge edge : frequentSubgraph) {
                // 获取当前边连接的两个节点
                GradoopId sourceId = edge.getSourceId();
                GradoopId targetId = edge.getTargetId();

                // 通过源节点 ID 和目标节点 ID 筛选边
                DataSet<EPGMEdge> edgesFromSource = allEdges.filter(e -> e.getSourceId().equals(sourceId));
                DataSet<EPGMEdge> edgesFromTarget = allEdges.filter(e -> e.getTargetId().equals(targetId));

                // 通过源节点和目标节点生成新的候选子图
                List<EPGMEdge> edgesFromSourceList = edgesFromSource.collect();
                List<EPGMEdge> edgesFromTargetList = edgesFromTarget.collect();

                for (EPGMEdge e : edgesFromSourceList) {
                    List<EPGMEdge> newSubgraph = new ArrayList<>(frequentSubgraph);
                    newSubgraph.add(e);
                    candidates.add(newSubgraph);
                }

                for (EPGMEdge e : edgesFromTargetList) {
                    List<EPGMEdge> newSubgraph = new ArrayList<>(frequentSubgraph);
                    newSubgraph.add(e);
                    candidates.add(newSubgraph);
                }
            }
        }

        return candidates;
    }

    private static boolean checkIsomorphism(Map<Integer, List<GeneralFrequentPath>> frequentSubgraphs, GeneralFrequentPath candidate) {
        for (List<GeneralFrequentPath> paths : frequentSubgraphs.values()) {
            for (GeneralFrequentPath path : paths) {
                if (candidate.equals(path)) {
                    return true; // 返回true表示candidate已经在frequentSubgraphs中出现过
                }
            }
        }
        return false; // 如果遍历完所有元素还没有找到匹配的，返回false
    }


    private static int countSupport(GeneralFrequentPath candidate,
                                    Map<String, Set<GradoopId>> labelToVertices,
                                    Map<GradoopId, Map<String, Integer>> vertexToPaths) {

        // 获取候选路径的起始和结束标签
        String startLabel = candidate.getStartNodeLabel();
        String endLabel = candidate.getEndNodeLabel();

        // 创建一个 Set 来避免重复计数的路径
        Set<String> countedPaths = new HashSet<>();

        // 从 labelToVertices map 中获取 startLabel 和 endLabel 的节点集
        Set<GradoopId> startVertices = labelToVertices.get(startLabel);
        Set<GradoopId> endVertices = labelToVertices.get(endLabel);

        // 创建一个变量来统计满足条件的路径数量
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

        // 返回满足条件的频繁路径数量
        return totalCount;
    }

    private static Set<GeneralFrequentPath> mergeFrequentSubgraphs(Map<Integer, List<GeneralFrequentPath>> frequentSubgraphs) {
        // 创建一个集合来存储所有的 GeneralFrequentPath 对象
        Set<GeneralFrequentPath> mergedSet = new HashSet<>();

        // 遍历 map 的值，并将每个列表的所有元素添加到合并后的集合中
        for (List<GeneralFrequentPath> paths : frequentSubgraphs.values()) {
            mergedSet.addAll(paths);
        }

        // 返回合并后的集合
        return mergedSet;
    }


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        LogicalGraph graph = TestData.loadTestData(env);
        graph.print();

        miningFrequentSubgraph(graph, env, 3);
    }

}