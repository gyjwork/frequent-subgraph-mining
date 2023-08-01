package com.ncl.gradoopgraph.utils;

import com.ncl.gradoopgraph.loadData.TestData;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import java.util.*;

/**
 * @author gyj
 * @title: TopologicalSort
 * @projectName gradoop-graph
 * @description: TODO
 * @date 6/12/23 4:11 PM
 */
public class TopologicalSort {

    // 这个方法实现了拓扑排序
    public static List<EPGMVertex> topologicalSort(LogicalGraph graph) throws Exception {
        // 获取图中的顶点和边的数据集
        DataSet<EPGMVertex> vertices = graph.getVertices();
        DataSet<EPGMEdge> edges = graph.getEdges();

        // 将数据集转换为普通的Java集合
        List<EPGMVertex> vertexList = vertices.collect();
        List<EPGMEdge> edgeList = edges.collect();

        // 创建一个HashMap来存储每个顶点的入度
        HashMap<GradoopId, Integer> inDegreeMap = new HashMap<>();
        for (EPGMEdge edge : edgeList) {
            GradoopId targetId = edge.getTargetId();
            inDegreeMap.put(targetId, inDegreeMap.getOrDefault(targetId, 0) + 1);
        }

        // 创建一个队列来存储所有入度为0的顶点
        Queue<EPGMVertex> queue = new LinkedList<>();
        for (EPGMVertex vertex : vertexList) {
            if (!inDegreeMap.containsKey(vertex.getId())) {
                queue.add(vertex);
            }
        }

        // 结果列表用于存储拓扑排序的顶点
        List<EPGMVertex> result = new ArrayList<>();
        while (!queue.isEmpty()) {
            EPGMVertex vertex = queue.poll();
            result.add(vertex);

            // 更新相邻顶点的入度
            for (EPGMEdge edge : edgeList) {
                if (edge.getSourceId().equals(vertex.getId())) {
                    GradoopId targetId = edge.getTargetId();
                    int inDegree = inDegreeMap.get(targetId) - 1;
                    if (inDegree == 0) {
                        queue.add(getVertexById(vertexList, targetId));
                        inDegreeMap.remove(targetId);
                    } else {
                        inDegreeMap.put(targetId, inDegree);
                    }
                }
            }
        }

        // 如果结果列表中的顶点数量不等于图中的顶点数量，则图中存在环，不是DAG
        if (result.size() != vertexList.size()) {
            throw new Exception("The graph is not a DAG");
        }

        // 返回拓扑排序后的有序顶点列表
        return result;
    }

    // 辅助方法，用于从列表中获取给定ID的顶点
    private static EPGMVertex getVertexById(List<EPGMVertex> vertices, GradoopId id) {
        for (EPGMVertex vertex : vertices) {
            if (vertex.getId().equals(id)) {
                return vertex;
            }
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        LogicalGraph graph = TestData.loadTestData(env);
        graph.print();

        List<EPGMVertex> topologicalSort = TopologicalSort.topologicalSort(graph);
        topologicalSort.stream().forEach(System.out::println);

    }


}
