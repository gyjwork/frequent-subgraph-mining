package com.ncl.gradoopgraph.utils;

import com.ncl.gradoopgraph.DIMSpan.DIMSpanAlgorithm;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.springframework.core.io.ClassPathResource;

/**
 * @author gyj
 * @title: GraphTransformerTest
 * @projectName gradoop-graph
 * @description: TODO
 * @date 5/17/232:40 AM
 */
public class GraphTransformerTest {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

        String inputPath = new ClassPathResource("timeSeriesGraph-20230427").getFile().getAbsolutePath();
        CSVDataSource dataSource = new CSVDataSource(inputPath, config);

        LogicalGraph inputGraph = dataSource.getLogicalGraph();

        //inputGraph.print();

        LogicalGraph transformedGraph = GraphTransformer.transformGraph(env,inputGraph);

        //transformedGraph.print();

        GraphCollection graphCollection = GraphTransformer.splitGraphByChainId(env,transformedGraph);

        graphCollection.print();

        // Set the DIMSpan algorithm parameters
        float minSupport = 1.0f;
        boolean directed = true;
        DataSet<GraphTransaction> frequentSubgraphs = DIMSpanAlgorithm.runDIMSpanAlgorithm(graphCollection, minSupport, directed);

        frequentSubgraphs.print();

    }
}
