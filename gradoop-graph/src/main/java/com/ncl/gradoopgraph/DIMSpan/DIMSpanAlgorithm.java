package com.ncl.gradoopgraph.DIMSpan;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.algorithms.fsm.dimspan.DIMSpan;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConfig;
import org.gradoop.flink.algorithms.fsm.dimspan.functions.conversion.EPGMGraphTransactionToLabeledGraph;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.LabeledGraphStringString;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

/**
 * @author gyj
 * @title: DIMSpanAlgorithm
 * @projectName gradoop-graph
 * @description: TODO
 * @date 5/4/233:38 AM
 */
public class DIMSpanAlgorithm {

    public static DataSet<GraphTransaction> runDIMSpanAlgorithm(GraphCollection inputGraphCollection, float minSupport, boolean directed) throws Exception {

        //GraphCollection inputGraphCollection = inputGraph.getConfig().getGraphCollectionFactory().fromDataSets(inputGraph.getGraphHead(), inputGraph.getVertices(), inputGraph.getEdges());

        // Set the DIMSpan algorithm parameters
        DIMSpanConfig dimSpanConfig = new DIMSpanConfig(minSupport, directed);

        // Create an instance of the DIMSpan algorithm
        DIMSpan dimSpan = new DIMSpan(dimSpanConfig);

        // Convert GraphCollection to DataSet<LabeledGraphStringString>
        DataSet<LabeledGraphStringString> inputDataSet = inputGraphCollection
                .getGraphTransactions()
                .map(new EPGMGraphTransactionToLabeledGraph());

        // Perform frequent subgraph mining
        DataSet<GraphTransaction> frequentSubgraphs = dimSpan.execute(inputDataSet);

        return frequentSubgraphs;
    }


}
