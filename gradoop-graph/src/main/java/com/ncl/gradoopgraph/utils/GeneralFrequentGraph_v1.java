package com.ncl.gradoopgraph.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.*;

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

    /**
     * 这个伪代码描述了一个基本的频繁子图挖掘算法。这类算法的目标是在给定的图数据库G中找到所有出现频次大于等于minsup（最小支持度）的子图。
     * 以下是伪代码的每一步骤的解释和举例：
     *
     * Populate F1：这一步初始化频繁子图集F1，包含所有在数据库中出现频次大于等于minsup的单个顶点子图。例如，如果我们有一个图数据库，其中顶点A出现了100次，顶点B出现了80次，顶点C出现了50次，如果我们设置minsup为60，那么F1就会包含顶点A和B，因为这两个顶点的出现频次大于等于60。
     *
     * while Fk not = 0：这一步是一个循环条件，如果当前的频繁子图集Fk为空，那么算法就停止。
     *
     * Ck+1 = Candidate generation(Fk, G)：在这一步，算法根据当前的频繁子图集Fk和图数据库G生成候选子图集Ck+1。这个过程通常是通过在Fk中的子图上添加顶点或者边来实现的。
     *
     * forall c € Ck+1：这一步是一个循环，对候选子图集Ck+1中的每一个子图c进行处理。
     *
     * if isomorphism checking(c) = true：在这一步，算法对子图c进行同构检查。同构检查的目的是确定是否存在一个和c同构的子图已经在前面的步骤中被计算过了。如果存在，那么我们就可以跳过这个子图，因为它的出现频次已经被计算过了。
     *
     * support counting(c, G)：这一步计算子图c在图数据库G中的出现频次。这通常是通过检查G中有多少子图和c同构来实现的。
     *
     * if c.sup > minsup：这一步检查子图c的出现频次是否大于等于最小支持度minsup。如果是，那么子图c就是一个频繁子图。
     *
     * Fk+1 = Fk+1 U{c}：这一步将子图c添加到频繁子图集Fk+1中。
     *
     * k = k +1：这一步增加计数器k，表示算法将进入下一轮循环，处理更大的子图。
     *
     * return Ui=1…k-1Fi：这一步返回所有找到的频繁子图的集合。
     *
     * 希望这个解释和举例能帮助你理解这个频繁子图挖掘算法的每一步骤。
     */

    // Fk maps to a collection of subgraphs (each represented as a list of edges)
    private Map<Integer, Set<List<EPGMEdge>>> frequentSubgraphs = new HashMap<>();


    /**
     *
     * to do : 应该是频繁模式的集合，而不是初始化的边集合，明日需要修改 ！！！
     */
    public LogicalGraph miningFrequentSubgraph(LogicalGraph graph, ExecutionEnvironment env, int minSup) throws Exception {
        GradoopFlinkConfig config = graph.getConfig();

        // Step 0: Populate F1
        frequentSubgraphs.put(1, generateF1(graph));

        // Step 1: Iterate until Fk is empty
        int k = 1;
        while (!frequentSubgraphs.get(k).isEmpty()) {
            // Step 2: Candidate generation
            Set<List<EPGMEdge>> candidates = generateCandidates(frequentSubgraphs.get(k), graph);

            Set<List<EPGMEdge>> nextFrequentSubgraphs = new HashSet<>();
            for (List<EPGMEdge> candidate : candidates) {
                // Step 4: Isomorphism checking
                if (checkIsomorphism(candidate)) {
                    // Step 5: Support counting
                    int support = countSupport(candidate, graph);
                    // Step 6: Check if support > minsup
                    if (support > minSup) {
                        nextFrequentSubgraphs.add(candidate);
                    }
                }
            }
            frequentSubgraphs.put(k + 1, nextFrequentSubgraphs);
            k += 1;
        }

        // Create the resulting graph
        LogicalGraph resultGraph = generateResultGraph(config, frequentSubgraphs, k);

        return resultGraph;
    }

    private Set<List<EPGMEdge>> generateF1(LogicalGraph graph) throws Exception {
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

    private Set<List<EPGMEdge>> generateCandidates(Set<List<EPGMEdge>> frequentSubgraphs, LogicalGraph graph) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    private boolean checkIsomorphism(List<EPGMEdge> candidate) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    private int countSupport(List<EPGMEdge> candidate, LogicalGraph graph) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    private LogicalGraph generateResultGraph(GradoopFlinkConfig config, Map<Integer, Set<List<EPGMEdge>>> frequentSubgraphs, int k) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

}