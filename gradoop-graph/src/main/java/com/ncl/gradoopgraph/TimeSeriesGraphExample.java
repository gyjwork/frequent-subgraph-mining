package com.ncl.gradoopgraph;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.springframework.core.io.ClassPathResource;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

/**
 * @author gyj
 * @title: TimeSeriesGraphExample
 * @projectName gradoop-graph
 * @description: TODO
 * @date 4/20/231:02 AM
 */
public class TimeSeriesGraphExample {

    public static void main(String[] args) throws Exception {
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
//
//        //String inputPath = new ClassPathResource("timeSeriesGraph").getFile().getAbsolutePath();
//        String inputPath = new ClassPathResource("timeSeriesGraph-20230427").getFile().getAbsolutePath();
//        CSVDataSource dataSource = new CSVDataSource(inputPath, config);
//
//        LogicalGraph logicalGraph = dataSource.getLogicalGraph();
//        logicalGraph.print();

        // Convert the original graph to a time series graph
        /**
        LogicalGraph timeSeriesGraph = convertToTimeSeries(logicalGraph);

        timeSeriesGraph.getGraphHead().print();
        timeSeriesGraph.getVertices().print();
        timeSeriesGraph.getEdges().print();
        timeSeriesGraph.print();
         */

    }

    public static LogicalGraph convertToTimeSeries(LogicalGraph logicalGraph) throws Exception{
        List<EPGMEdge> newEdges = new ArrayList<>();

        for (EPGMEdge edge : logicalGraph.getEdges().collect()) {
            // Check for the presence of a time interval property
            if (edge.hasProperty("__valFrom") && edge.hasProperty("__valTo")) {
                // Splitting the time interval
                long from = edge.getPropertyValue("__valFrom").getLong();
                long to = edge.getPropertyValue("__valTo").getLong();

                long year = Instant.ofEpochMilli(from)
                        .atZone(ZoneId.systemDefault())
                        .getYear();

                long endYear = Instant.ofEpochMilli(to)
                        .atZone(ZoneId.systemDefault())
                        .getYear();

                while (year < endYear) {
                    EPGMEdge newEdge = new EPGMEdge();

                    // Generate a unique ID for the new edge based on the original edge ID and the current year
                    String seedString = edge.getId().toString() + "-" + year + UUID.randomUUID();
                    long seed = seedString.hashCode();
                    GradoopId newEdgeId = getGeneratedId(seed);
                    newEdge.setId(newEdgeId);

                    newEdge.setLabel(edge.getLabel());
                    newEdge.setSourceId(edge.getSourceId());
                    newEdge.setTargetId(edge.getTargetId());

                    edge.getProperties().forEach(newEdge::setProperty);

                    // Set the new time attribute
                    long newFrom = getMillisFromYear(year);
                    long newTo = getMillisFromYear(year + 1);
                    newEdge.setProperty("__valFrom", newFrom);
                    newEdge.setProperty("__valTo", newTo);

                    // Set the graphic ID associated with the original edge
                    newEdge.setGraphIds(edge.getGraphIds());

                    newEdges.add(newEdge);
                    year++;
                }
            } else {
                // If there is no time interval attribute, keep the original edge
                newEdges.add(edge);
            }
        }

        // Replace the original edge
        return logicalGraph.getConfig().getLogicalGraphFactory()
                .fromDataSets(logicalGraph.getGraphHead(), logicalGraph.getVertices(), logicalGraph.getConfig().getExecutionEnvironment().fromCollection(newEdges));
    }

    private static long getMillisFromYear(long year) {
        return LocalDate.of((int) year, 1, 1)
                .atStartOfDay(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
    }

    private static GradoopId getGeneratedId(long seed) {
        Random random = new Random(seed);
        byte[] idBytes = new byte[GradoopId.ID_SIZE];
        random.nextBytes(idBytes);
        return GradoopId.fromByteArray(idBytes);
    }


}
