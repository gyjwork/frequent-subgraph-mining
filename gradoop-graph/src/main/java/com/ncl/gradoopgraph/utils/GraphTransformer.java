package com.ncl.gradoopgraph.utils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.springframework.core.io.ClassPathResource;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author gyj
 * @title: GraphTransformer
 * @projectName gradoop-graph
 * @description: TODO
 * @date 4/28/234:27 AM
 */

public class GraphTransformer {

    public static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // Define the static variable edgeType
    private static final Map<String, EdgeTypeInfo> EDGE_TYPE;

    // Use static initialization blocks to populate edgeType variables
    static {
        EDGE_TYPE = new HashMap<>();
        EDGE_TYPE.put("buys", new EdgeTypeInfo(1, true));
        EDGE_TYPE.put("owns", new EdgeTypeInfo(2, true));
        EDGE_TYPE.put("sells", new EdgeTypeInfo(3, true));
        EDGE_TYPE.put("transfers_ownership", new EdgeTypeInfo(4, false));
    }

    public static LogicalGraph transformGraph(LogicalGraph inputGraph) throws Exception {

        // Merge source and target vertices and create new vertices
        List<EPGMVertex> mergedVerticesList = generateNewVertices(inputGraph).collect();

        DataSet<EPGMEdge> newEdges = generateNewEdges(mergedVerticesList, EDGE_TYPE);

        // Create a new LogicalGraph with new vertices and new edges
        LogicalGraph transformedGraph = inputGraph.getConfig().getLogicalGraphFactory().fromDataSets(env.fromCollection(mergedVerticesList), newEdges);

        return transformedGraph;
    }

    public static DataSet<EPGMVertex> generateNewVertices(LogicalGraph inputGraph) throws Exception {
        Map<GradoopId, EPGMVertex> verticesMap = inputGraph.getVertices().collect().stream()
                .collect(Collectors.toMap(EPGMVertex::getId, Function.identity()));

        // Merge source and target vertices and create new vertices
        DataSet<EPGMVertex> mergedVertices = inputGraph.getEdges().flatMap(new FlatMapFunction<EPGMEdge, EPGMVertex>() {
            @Override
            public void flatMap(EPGMEdge edge, Collector<EPGMVertex> out) throws Exception {
                GradoopId sourceId = edge.getSourceId();
                GradoopId targetId = edge.getTargetId();
                String newLabel = edge.getLabel();

                // Get the properties of the source and target vertices
                Properties properties = new Properties();
                EPGMVertex sourceVertex = verticesMap.get(sourceId);
                EPGMVertex targetVertex = verticesMap.get(targetId);

                // Add the attributes of the source and target vertices to the new set of attributes
                for (String key : sourceVertex.getPropertyKeys()) {
                    properties.set("source_" + key, sourceVertex.getPropertyValue(key));
                }
                for (String key : targetVertex.getPropertyKeys()) {
                    properties.set("target_" + key, targetVertex.getPropertyValue(key));
                }

                // Add all attributes of the side to the new set of attributes
                for (String key : edge.getPropertyKeys()) {
                    properties.set("edge_" + key, edge.getPropertyValue(key));
                }

                // Add the IDs of the original start and end points to the properties of the new vertex
                properties.set("originalSourceId", PropertyValue.create(sourceId));
                properties.set("originalTargetId", PropertyValue.create(targetId));

                // Create a new vertex with a new label and attributes
                GradoopId newId = GradoopId.get();

                EPGMVertex newVertex = new EPGMVertex();
                newVertex.setId(newId);
                newVertex.setLabel(newLabel);
                newVertex.setProperties(properties);
                out.collect(newVertex);
            }
        });

        return mergedVertices;
    }

    public static DataSet<EPGMEdge> generateNewEdges(List<EPGMVertex> mergedVerticesList, Map<String, EdgeTypeInfo> edgeType) throws Exception {

        // Store a list of new vertices with the same source vertex, using the ID of the source vertex as the key
        Map<GradoopId, List<NewVertex>> vertexMapBySource = new HashMap<>();

        // Traversing the merged vertex list
        for (EPGMVertex vertex : mergedVerticesList) {

            // Get the ID of the original source vertex and the target vertex
            GradoopId originalSourceId = vertex.getPropertyValue("originalSourceId").getGradoopId();
            GradoopId originalTargetId = vertex.getPropertyValue("originalTargetId").getGradoopId();

            // Get the timestamp
            PropertyValue valFrom = vertex.getPropertyValue("edge___valFrom");
            PropertyValue valAt = vertex.getPropertyValue("edge___valAt");
            long timestamp = 0;

            if (valFrom != null) {
                timestamp = valFrom.getLong();
            } else if (valAt != null) {
                timestamp = valAt.getLong();
            }

            // Get the label, priority and directedness information of the edge
            String edgeLabel = vertex.getLabel();
            int priority = edgeType.get(edgeLabel).getPriority();
            boolean isDirected = edgeType.get(edgeLabel).isDirected();

            // Create a new vertex object containing the original vertex, timestamp and priority information
            NewVertex newVertex = new NewVertex(vertex, timestamp, priority);

            // Add the new vertex to the list corresponding to the source vertex ID
            vertexMapBySource.computeIfAbsent(originalSourceId, k -> new ArrayList<>()).add(newVertex);

            // If the edge is an undirected edge, add the vertex to the list corresponding to the target vertex ID
            if (!isDirected) {
                vertexMapBySource.computeIfAbsent(originalTargetId, k -> new ArrayList<>()).add(newVertex);
            }
        }

        // Sort the list of new vertices corresponding to each source vertex ID by timestamp and priority
        for (List<NewVertex> vertices : vertexMapBySource.values()) {
            vertices.sort(Comparator.comparing(NewVertex::getTimestamp).thenComparing(NewVertex::getPriority));
        }

        // A list for storing the new edges generated
        List<EPGMEdge> allEdges = new ArrayList<>();

        // Iterate through the list of new vertices corresponding to each source vertex ID
        for (List<NewVertex> vertices : vertexMapBySource.values()) {
            List<EPGMEdge> edges = new ArrayList<>();

            // Create new edges based on adjacent new vertices
            for (int i = 0; i < vertices.size() - 1; i++) {
                NewVertex source = vertices.get(i);
                NewVertex target = vertices.get(i + 1);

                GradoopId newId = GradoopId.get();

                // Create a new edge object
                EPGMEdge newEdge = new EPGMEdge(newId, "newEdgeLabel", source.vertex.getId(), target.vertex.getId(), new Properties(), new GradoopIdSet());
                edges.add(newEdge);
            }

            // Add the newly generated edges to the total edge list
            allEdges.addAll(edges);
        }

        // Convert all newly generated edges to DataSet<EPGMEdge> objects and return
        return env.fromCollection(allEdges);
    }

    public static void main(String[] args) throws Exception {

        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
        String inputPath = new ClassPathResource("timeSeriesGraph-20230427").getFile().getAbsolutePath();
        CSVDataSource dataSource = new CSVDataSource(inputPath, config);

        LogicalGraph inputGraph = dataSource.getLogicalGraph();

        inputGraph.print();

        LogicalGraph transformedGraph = transformGraph(inputGraph);

        transformedGraph.print();
    }
}

