package com.ncl.gradoopgraph;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.EdgeCount;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.temporal.io.api.TemporalDataSource;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSource;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.functions.predicates.AsOf;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.CNFPostProcessing;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.dummy.DummyTemporalGraphStatisticsFactory;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.CypherTemporalPatternMatching;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopConfig;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.springframework.core.io.ClassPathResource;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * @author gyj
 * @title: TemporalGradoopCSVLoader
 * @projectName gradoop-graph
 * @description: TODO
 * @date 4/12/235:04 AM
 */
public class TemporalGradoopCSVLoader {
    public static void main(String[] args) throws Exception {
        String inputPath = new ClassPathResource("data").getFile().getAbsolutePath();

        TemporalGraph temporalGraph = loadTemporalGraphFromCSV(inputPath);

        System.out.println("[getVertices]:"+temporalGraph.getVertices().count()+"[getEdges]:"+temporalGraph.getEdges().count());

        // 1. Subgraph
        subgraphExample(temporalGraph);

        // 2. Aggregate
        aggregateExample(temporalGraph);

        // 3. GroupBy
        groupByExample(temporalGraph);

        // 4. Snapshot
        snapshotExample(temporalGraph);

        // 5. TemporalGrouping
        temporalGroupingExample(temporalGraph);

        // 6. Pattern Matching
        patternMatchingExample(temporalGraph);

    }

    public static TemporalGraph loadTemporalGraphFromCSV(String inputPath) throws Exception {
        // Set up Flink execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        TemporalGradoopConfig config = TemporalGradoopConfig.createConfig(env);

        // Define the TemporalCSV data source
        TemporalDataSource dataSource = new TemporalCSVDataSource(inputPath, config);
        TemporalGraph temporalGraph = dataSource.getTemporalGraph();

        return temporalGraph;
    }

    public static void snapshotExample(TemporalGraph temporalGraph) throws Exception {
        // We need a UNIX timestamp in milliseconds since epoch.
        long queryTimestamp = LocalDateTime
                .of(2018, 12, 24, 20, 15, 0, 0)
                .toInstant(ZoneOffset.UTC)
                .toEpochMilli();

        // Get the graph 'as of' our query timestamp (considers the valid time dimension as default)
        TemporalGraph historicalGraph = temporalGraph.snapshot(new AsOf(queryTimestamp));

        // Do something with the historicalGraph, e.g., print the vertex and edge counts
        System.out.println("Snapshot Graph:");
        System.out.println("Vertex count: " + historicalGraph.getVertices().count());
        System.out.println("Edge count: " + historicalGraph.getEdges().count());
    }

    public static void temporalGroupingExample(TemporalGraph temporalGraph) throws Exception {
        // Use Temporal Grouping
        TemporalGraph groupedGraph = temporalGraph.callForGraph(
                new Grouping.GroupingBuilder()
                        .setStrategy(GroupingStrategy.GROUP_REDUCE)
                        .addVertexGroupingKey("city")
                        .addEdgeGroupingKey("__label__")
                        .addEdgeAggregateFunction(new EdgeCount("cnt"))
                        .build()
        );

        // Do something with the groupedGraph, e.g., print the vertex and edge counts
        System.out.println("Grouped Graph:");
        System.out.println("Vertex count: " + groupedGraph.getVertices().count());
        System.out.println("Edge count: " + groupedGraph.getEdges().count());

        // Convert the grouped TemporalGraph to a LogicalGraph
        LogicalGraph logicalGraph = groupedGraph.toLogicalGraph();

        // Print the vertex and edge properties to the console
        System.out.println("Grouped Graph Vertices:");
        logicalGraph.getVertices().print();

        System.out.println("Grouped Graph Edges:");
        logicalGraph.getEdges().print();
    }


    public static void subgraphExample(TemporalGraph temporalGraph) throws Exception {
        // Extract subgraph where vertex property "age" is greater than or equal to 30,
        // and edge property "since" is greater than or equal to 2014
        TemporalGraph subgraph = temporalGraph.subgraph(
                v -> v.hasProperty("age") && v.getPropertyValue("age").getInt() >= 30,
                e -> e.hasProperty("since") && e.getPropertyValue("since").getInt() >= 2014
        );

        System.out.println("Subgraph:");
        System.out.println("Vertices: " + subgraph.getVertices().count());
        System.out.println("Edges: " + subgraph.getEdges().count());
    }

    private static void aggregateExample(TemporalGraph temporalGraph) throws Exception {
        DataSet<EPGMVertex> vertices = temporalGraph.getVertices()
                .filter(vertex -> vertex.getLabel().equals("Person"))
                .map(new TemporalVertexToEPGMVertex());

        long vertexCount = vertices.count();
        double averageAge = vertices
                .map(vertex -> vertex.getPropertyValue("age").getInt())
                .reduce((a, b) -> a + b)
                .map(new CalculateAverageAge(vertexCount))
                .collect()
                .get(0);

        System.out.println("Aggregate:");
        System.out.println("Average age: " + averageAge);
    }

    private static class TemporalVertexToEPGMVertex implements MapFunction<TemporalVertex, EPGMVertex> {
        @Override
        public EPGMVertex map(TemporalVertex temporalVertex) {
            GradoopId id = temporalVertex.getId();
            String label = temporalVertex.getLabel();
            Properties properties = temporalVertex.getProperties();
            GradoopIdSet graphIds = temporalVertex.getGraphIds();
            return new EPGMVertex(id, label, properties, graphIds);
        }
    }

    private static class CalculateAverageAge implements MapFunction<Integer, Double> {
        private final long vertexCount;

        public CalculateAverageAge(long vertexCount) {
            this.vertexCount = vertexCount;
        }

        @Override
        public Double map(Integer value) {
            return (double) value / vertexCount;
        }
    }


    public static void groupByExample(TemporalGraph temporalGraph) throws Exception {
        // Filter vertices with "Person" label
        DataSet<TemporalVertex> personVertices = temporalGraph.getVertices()
                .filter(vertex -> vertex.getLabel().equals("Person"));

        // Group vertices by their "city" property and count them
        GroupReduceOperator<TemporalVertex, Tuple2<String, Integer>> cityCounts = personVertices
                .groupBy(vertex -> vertex.getPropertyValue("city").toString())
                .reduceGroup(new GroupReduceFunction<TemporalVertex, Tuple2<String, Integer>>() {
                    @Override
                    public void reduce(Iterable<TemporalVertex> vertices, Collector<Tuple2<String, Integer>> out) {
                        int count = 0;
                        String city = null;
                        for (TemporalVertex vertex : vertices) {
                            if (city == null) {
                                city = vertex.getPropertyValue("city").toString();
                            }
                            count++;
                        }
                        out.collect(new Tuple2<>(city, count));
                    }
                });

        System.out.println("City counts:");
        for (Tuple2<String, Integer> cityCount : cityCounts.collect()) {
            System.out.println("City: " + cityCount.f0 + ", Count: " + cityCount.f1);
        }
    }

    public static void patternMatchingExample(TemporalGraph temporalGraph) throws Exception {
        // Define the query pattern
        String queryPattern = "MATCH (person1:Person)-[r]->(person2:Person) WHERE r.since >= 2014";

        // Instantiate CypherTemporalPatternMatching with the required parameters
        CypherTemporalPatternMatching cypherTemporalPatternMatching = new CypherTemporalPatternMatching(
                queryPattern,
                true,
                MatchStrategy.HOMOMORPHISM,
                MatchStrategy.HOMOMORPHISM,
                new DummyTemporalGraphStatisticsFactory().fromGraph(temporalGraph),
                new CNFPostProcessing()
        );

        // Apply Cypher Temporal Pattern Matching
        TemporalGraphCollection temporalGraphCollection = cypherTemporalPatternMatching.execute(temporalGraph);

        // Print the results
        System.out.println("Pattern Matching:");
        System.out.println("Vertices count: " + temporalGraphCollection.getVertices().count());
        System.out.println("Edges count: " + temporalGraphCollection.getEdges().count());

        // Print the matched vertices
        System.out.println("Matched Vertices:");
        temporalGraphCollection.getVertices().print();

        // Print the matched edges.csv
        System.out.println("Matched Edges:");
        temporalGraphCollection.getEdges().print();
    }
}


