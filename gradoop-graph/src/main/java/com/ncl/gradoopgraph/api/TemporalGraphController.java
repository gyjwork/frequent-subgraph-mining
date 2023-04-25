package com.ncl.gradoopgraph.api;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSource;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.util.TemporalGradoopConfig;
import org.springframework.core.io.ClassPathResource;

/**
 * @author gyj
 * @title: TemporalGraphController
 * @projectName gradoop
 * @description: TODO
 * @date 4/13/232:55 AM
 */
public class TemporalGraphController {

    public static void main(String[] args) {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            String inputPath = new ClassPathResource("data").getFile().getAbsolutePath();

            System.out.println(inputPath);

            TemporalGradoopConfig config = TemporalGradoopConfig.createConfig(env);

            TemporalCSVDataSource dataSource = new TemporalCSVDataSource(inputPath, config);
            TemporalGraph temporalGraph = dataSource.getTemporalGraph();

            long vertexCount = temporalGraph.getVertices().count();
            long edgeCount = temporalGraph.getEdges().count();

            System.out.println("Vertex count:" + vertexCount);
            System.out.println("Edge count:" + edgeCount);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}