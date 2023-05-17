package com.ncl.gradoopgraph;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.algorithms.fsm.dimspan.DIMSpan;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConfig;
import org.gradoop.flink.algorithms.fsm.dimspan.functions.conversion.EPGMGraphTransactionToLabeledGraph;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.LabeledGraphStringString;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.flink.io.api.DataSource;
import org.springframework.core.io.ClassPathResource;

import java.util.List;
import java.util.Set;


/**
 * @author gyj
 * @title: test
 * @projectName gradoop-graph
 * @description: TODO
 * @date 4/20/232:42 AM
 */
public class GradoopDIMSpanExample {

    public static void main(String[] args) throws Exception {

        /**
         * Buyer (Alice) -buys-> Car (Tesla Model 3)
         * Seller (David) -auctions-> Car (Tesla Model 3)
         *
         * Buyer (Bob) -buys-> Car (BMW M3)
         * Seller (Charlie) -auctions-> Car (BMW M3)
         */

        String inputPath = new ClassPathResource("dimspanData").getFile().getAbsolutePath();
        //String inputPath = new ClassPathResource("dimspanDataNew").getFile().getAbsolutePath();

        //String inputPath = new ClassPathResource("data").getFile().getAbsolutePath();
        //TemporalGraph temporalGraph = loadTemporalGraphFromCSV(inputPath);

        // Initialize the execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Set up Gradoop Flink configuration
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

        // Load diagram data from CSV file
        DataSource dataSource = new CSVDataSource(inputPath, config);
        GraphCollection graphCollection = dataSource.getGraphCollection();
//        graphCollection.print();
//
//        DataSet<GraphTransaction> transactions = graphCollection.getGraphTransactions();
//
//        transactions.collect().forEach(transaction -> {
//            // Do something with each transaction
//            EPGMGraphHead graphHead = transaction.getGraphHead();
//            Set<EPGMVertex> vertices = transaction.getVertices();
//            Set<EPGMEdge> edges = transaction.getEdges();
//            System.out.println("Graph Id: " + graphHead.getId());
//            System.out.println("Vertices: " + vertices);
//            System.out.println("Edges: " + edges);
//        });

        // Set the DIMSpan algorithm parameters
        float minSupport = 0.5f;
        boolean directed = true;

        // Create DIMSpanConfig
        DIMSpanConfig dimSpanConfig = new DIMSpanConfig(minSupport, directed);

        // Create an instance of the DIMSpan algorithm
        DIMSpan dimSpan = new DIMSpan(dimSpanConfig);

        // Convert GraphCollection to DataSet<LabeledGraphStringString>
        DataSet<LabeledGraphStringString> inputDataSet = graphCollection
                .getGraphTransactions()
                .map(new EPGMGraphTransactionToLabeledGraph());

        System.out.println("---------------------getGraphHeads---------------------");
        graphCollection.getGraphHeads().print();

        System.out.println("---------------------getVertices---------------------");
        graphCollection.getVertices().print();

        System.out.println("---------------------getEdges---------------------");
        graphCollection.getEdges().print();

        System.out.println("---------------------graphCollection---------------------");
        graphCollection
                .getGraphTransactions()
                .map(transaction -> transaction.toString())
                .print();

        System.out.println("---------------------frequentSubgraphs---------------------");

        // Perform frequent subgraph mining
        DataSet<GraphTransaction> frequentSubgraphs = dimSpan.execute(inputDataSet);

        // Output frequent subgraph results
        try {
            List<GraphTransaction> frequentSubgraphsList = frequentSubgraphs.collect();
            if (!frequentSubgraphsList.isEmpty()) {
                for (GraphTransaction frequentSubgraph : frequentSubgraphsList) {
                    System.out.println(frequentSubgraph);
                }
            } else {
                System.out.println("No frequent subgraphs found that match the minimum support");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
