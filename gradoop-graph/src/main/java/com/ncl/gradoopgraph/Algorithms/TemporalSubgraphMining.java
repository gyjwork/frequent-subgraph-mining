package com.ncl.gradoopgraph.Algorithms;

import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author gyj
 * @title: TemporalSubgraphMining
 * @projectName gradoop-graph
 * @description: TODO
 * @date 6/12/233:00 AM
 */
public class TemporalSubgraphMining {

    // 1. Assume that there are no loops in the graph, as we only combine edges into future events
    // In the data pre-processing phase we need to ensure this

    // 2. Topological sorting get the starting point
    public List<EPGMVertex> topologicalSorting(LogicalGraph graph) throws Exception {
        // This implements the topological sorting algorithm and returns a list of starting points, see the pseudo code above for details
        // ...
        return null;
    }

    // 3. Start searching based on the starting point in the previous step, graph search algorithm implementation
    public List<List<EPGMVertex>> graphSearch(LogicalGraph graph, List<EPGMVertex> startPoints) {
        // The graph search algorithm is implemented here and returns all paths searched
        // ...
        return null;
    }

    // 4. Algorithm to find all paths AllsimplePaths
    // This step is actually already done in the graph search algorithm

    // 5. Table data structure, recording the number of occurrences of each node in the graph
    public Map<EPGMVertex, Integer> countVerticesOccurrences(List<List<EPGMVertex>> paths) {
        Map<EPGMVertex, Integer> verticesOccurrences = new HashMap<>();
        for (List<EPGMVertex> path : paths) {
            for (EPGMVertex vertex : path) {
                verticesOccurrences.put(vertex, verticesOccurrences.getOrDefault(vertex, 0) + 1);
            }
        }
        return verticesOccurrences;
    }

    // 6. Set up the minsupport algorithm to find a number of points such as A... (n points)... ...C... (n points)... .F
    public List<List<EPGMVertex>> findFrequentPaths(List<List<EPGMVertex>> paths, Map<EPGMVertex, Integer> verticesOccurrences, int minSupport) {
        List<List<EPGMVertex>> frequentPaths = new ArrayList<>();
        for (List<EPGMVertex> path : paths) {
            boolean isFrequent = true;
            for (EPGMVertex vertex : path) {
                if (verticesOccurrences.get(vertex) < minSupport) {
                    isFrequent = false;
                    break;
                }
            }
            if (isFrequent) {
                frequentPaths.add(path);
            }
        }
        return frequentPaths;
    }

    public void temporalSubgraphMining(LogicalGraph graph, int minSupport) throws Exception {
        List<EPGMVertex> startPoints = topologicalSorting(graph);
        List<List<EPGMVertex>> paths = graphSearch(graph, startPoints);
        Map<EPGMVertex, Integer> verticesOccurrences = countVerticesOccurrences(paths);
        List<List<EPGMVertex>> frequentPaths = findFrequentPaths(paths, verticesOccurrences, minSupport);
        // frequent paths are handled here
        // ...
    }
}

