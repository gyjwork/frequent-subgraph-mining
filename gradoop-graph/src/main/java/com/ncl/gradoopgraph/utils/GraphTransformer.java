package com.ncl.gradoopgraph.utils;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author gyj
 * @title: GraphTransformer
 * @projectName gradoop-graph
 * @description: This class provides utility methods for transforming and splitting a graph using Gradoop and Flink.
 * The transformation process involves merging vertices based on common edges, and generating new edges based on the order of these merged vertices.
 * The split process involves grouping edges based on a chainId and creating subgraphs.
 * @date 4/28/234:27 AM
 */

public class GraphTransformer {

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

    /**
     * This method transforms an input graph by merging vertices and generating new edges.
     * @param env The Flink execution environment.
     * @param inputGraph The input graph to be transformed.
     * @return The transformed graph.
     * @throws Exception
     */
    public static LogicalGraph transformGraph(ExecutionEnvironment env, LogicalGraph inputGraph) throws Exception {

        // Merge source and target vertices and create new vertices
        List<EPGMVertex> mergedVerticesList = generateNewVertices(inputGraph).collect();

        DataSet<EPGMEdge> newEdges = generateNewEdges(env, mergedVerticesList, EDGE_TYPE);

        // Create a new LogicalGraph with new vertices and new edges
        LogicalGraph transformedGraph = inputGraph.getConfig().getLogicalGraphFactory().fromDataSets(env.fromCollection(mergedVerticesList), newEdges);

        return transformedGraph;
    }

    /**
     * This method merges source and target vertices based on common edges and generates new vertices.
     * @param inputGraph The input graph.
     * @return A DataSet of the new vertices.
     * @throws Exception
     */
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

    /**
     * This method generates new edges based on the order of vertices in mergedVerticesList and the edge type information.
     * @param env The Flink execution environment.
     * @param mergedVerticesList The list of merged vertices.
     * @param edgeType The edge type information.
     * @return A DataSet of the new edges.
     * @throws Exception
     */
    public static DataSet<EPGMEdge> generateNewEdges(ExecutionEnvironment env, List<EPGMVertex> mergedVerticesList, Map<String, EdgeTypeInfo> edgeType) throws Exception {

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

        // A counter for chainId
        int chainId = 0;

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

                // Set chainId property
                newEdge.setProperty("chainId", chainId);

                edges.add(newEdge);
            }

            // Increment chainId counter for the next chain
            chainId++;

            // Add the newly generated edges to the total edge list
            allEdges.addAll(edges);
        }

        // Convert all newly generated edges to DataSet<EPGMEdge> objects and return
        return env.fromCollection(allEdges);
    }

    /**
     * This method splits a graph into subgraphs based on the chainId property of the edges.
     * @param env The Flink execution environment.
     * @param transformedGraph The graph to be split.
     * @return A GraphCollection of the subgraphs.
     * @throws Exception
     */
    public static GraphCollection splitGraphByChainId(ExecutionEnvironment env, LogicalGraph transformedGraph) throws Exception {

        // Get the vertices and edges of the transformed graph
        DataSet<EPGMVertex> vertices = transformedGraph.getVertices();
        DataSet<EPGMEdge> edges = transformedGraph.getEdges();

        // Create a map to group edges by chain id
        Map<Integer, List<EPGMEdge>> groupedEdges = new HashMap<>();
        // Create a map to keep track of the chain ids for each vertex
        Map<GradoopId, Set<Integer>> vertexChainIds = new HashMap<>();

        // Iterate over the edges
        for (EPGMEdge edge : edges.collect()) {
            int chainId = edge.getPropertyValue("chainId").getInt();

            // Add the edge to the list of edges for the current chain id
            groupedEdges.computeIfAbsent(chainId, k -> new ArrayList<>()).add(edge);

            // Track chain ids for each vertex
            GradoopId sourceId = edge.getSourceId();
            GradoopId targetId = edge.getTargetId();

            // Add the chain id to the set of chain ids for the current source and target vertices
            vertexChainIds.computeIfAbsent(sourceId, k -> new HashSet<>()).add(chainId);
            vertexChainIds.computeIfAbsent(targetId, k -> new HashSet<>()).add(chainId);
        }

        // Create a map of graph heads, one for each chain id
        Map<Integer, EPGMGraphHead> groupedGraphHead = groupedEdges.keySet().stream()
                .collect(Collectors.toMap(key -> key, key -> new EPGMGraphHead(GradoopId.get(), "g" + key, new Properties())));

        // List to hold the graph transactions
        List<GraphTransaction> graphTransactions = new ArrayList<>();

        // Iterate over the entries in the groupedEdges map
        for (Map.Entry<Integer, List<EPGMEdge>> entry : groupedEdges.entrySet()) {
            List<EPGMEdge> edgeList = entry.getValue();

            // Create a DataSet of edges for the subgraph associated with the current chain id
            DataSet<EPGMEdge> subgraphEdgesDataSet = env.fromCollection(edgeList)
                    .map(new MapFunction<EPGMEdge, EPGMEdge>() {
                        @Override
                        public EPGMEdge map(EPGMEdge edge) throws Exception {
                            int chainId = edge.getPropertyValue("chainId").getInt();
                            GradoopId graphId = groupedGraphHead.get(chainId).getId();
                            // Add the graph id to the edge
                            edge.addGraphId(graphId);
                            return edge;
                        }
                    });

            // Create a set of vertex ids for the current subgraph
            Set<GradoopId> vertexIds = new HashSet<>();
            for (EPGMEdge edge : edgeList) {
                vertexIds.add(edge.getSourceId());
                vertexIds.add(edge.getTargetId());
            }

            // Create a DataSet of vertices for the current subgraph
            DataSet<EPGMVertex> subgraphVerticesDataSet = vertices.filter(new FilterFunction<EPGMVertex>() {
                @Override
                public boolean filter(EPGMVertex vertex) {
                    return vertexIds.contains(vertex.getId());
                }
            }).map(vertex -> {
                GradoopIdSet graphIds = new GradoopIdSet();
                for (Integer chainId : vertexChainIds.get(vertex.getId())) {
                    graphIds.add(groupedGraphHead.get(chainId).getId());
                }
                return new EPGMVertex(vertex.getId(), vertex.getLabel(), vertex.getProperties(), graphIds);
            });

            // Create a graph transaction for the current subgraph
            GraphTransaction graphTransaction = new GraphTransaction(groupedGraphHead.get(entry.getKey()), new HashSet<>(subgraphVerticesDataSet.collect()), new HashSet<>(subgraphEdgesDataSet.collect()));

            // Add the graph transaction to the list of graph transactions
            graphTransactions.add(graphTransaction);
        }

        // Convert the list of graph transactions into a DataSet
        DataSet<GraphTransaction> graphTransactionsDataSet = env.fromCollection(graphTransactions);

        // Create a GraphCollection from the DataSet of graph transactions
        GraphCollection graphCollection = transformedGraph.getConfig().getGraphCollectionFactory().fromTransactions(graphTransactionsDataSet);

        // Return the GraphCollection
        return graphCollection;
    }


}

