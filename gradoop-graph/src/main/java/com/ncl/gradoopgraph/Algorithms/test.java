package com.ncl.gradoopgraph.Algorithms;

import com.ncl.gradoopgraph.loadData.TestData;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import java.util.Map;

/**
 * @author gyj
 * @title: test
 * @projectName gradoop-graph
 * @description: TODO
 * @date 8/12/232:27 AM
 */
public class test {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        LogicalGraph graph = TestData.loadTestData(env);
        //graph.print();

        long cnt = 0;
        int i = 30;
        //for (int i = 3 ; i <  7 ; i++) {

        long startTime = System.nanoTime();

        Map<Integer, LogicalGraph> frequentSubGraph = GFSM.run(graph, i);
        long endTime = System.nanoTime();
        long durationInNano = endTime - startTime;
        double durationInSeconds = (double) durationInNano / 1_000_000_000.0;

        System.out.println("minSup：" + i);
        System.out.println("算法运行时间（秒）：" + durationInSeconds);

        for (Map.Entry<Integer, LogicalGraph> entry : frequentSubGraph.entrySet()) {
            if (entry.getValue().getEdges().count() > cnt) {
                cnt = entry.getValue().getEdges().count();
            }
        }
        System.out.println("频繁子图数量 ：" + cnt);
        cnt = 0;
       // }
    }
}
