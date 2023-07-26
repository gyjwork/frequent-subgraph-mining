package com.ncl.gradoopgraph.Beans;

import java.util.List;

/**
 * @author gyj
 * @title: GeneralFrequentPath
 * @projectName gradoop-graph
 * @description: TODO
 * @date 7/17/23 3:27 PM
 */
public class GeneralFrequentPath {
    private String startNodeLabel;
    private List<Integer> edgeIds;
    private String endNodeLabel;

    public GeneralFrequentPath(String startNodeLabel, List<Integer> edgeIds, String endNodeLabel) {
        this.startNodeLabel = startNodeLabel;
        this.edgeIds = edgeIds;
        this.endNodeLabel = endNodeLabel;
    }


    public boolean containsEdgeIdsOf(GeneralFrequentPath other) {
        return this.edgeIds.containsAll(other.edgeIds);
    }

    public String getStartNodeLabel() {
        return startNodeLabel;
    }

    public void setStartNodeLabel(String startNodeLabel) {
        this.startNodeLabel = startNodeLabel;
    }

    public List<Integer> getEdgeIds() {
        return edgeIds;
    }

    public void setEdgeIds(List<Integer> edgeIds) {
        this.edgeIds = edgeIds;
    }

    public String getEndNodeLabel() {
        return endNodeLabel;
    }

    public void setEndNodeLabel(String endNodeLabel) {
        this.endNodeLabel = endNodeLabel;
    }

    @Override
    public String toString() {
        return "GeneralFrequentPath{" +
                "startNodeLabel='" + startNodeLabel + '\'' +
                ", edgeIds=" + edgeIds +
                ", endNodeLabel='" + endNodeLabel + '\'' +
                '}';
    }
}
