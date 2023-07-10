package com.ncl.gradoopgraph.loadData;

import com.ncl.gradoopgraph.utils.GraphTransformer;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;

/**
 * @author gyj
 * @title: TestData
 * @projectName gradoop-graph
 * @description: TODO
 * @date 7/10/235:11 PM
 */
public class TestData {

    public static LogicalGraph loadTestData (ExecutionEnvironment env){

        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

        String inputPath = null;
        try {
            inputPath = new ClassPathResource("timeSeriesGraph-20230427").getFile().getAbsolutePath();
        } catch (IOException e) {
            e.printStackTrace();
        }
        CSVDataSource dataSource = new CSVDataSource(inputPath, config);

        LogicalGraph inputGraph = dataSource.getLogicalGraph();
        LogicalGraph graph = null;
        try {
            graph = GraphTransformer.transformGraph(env, inputGraph);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return graph;
    }
}
