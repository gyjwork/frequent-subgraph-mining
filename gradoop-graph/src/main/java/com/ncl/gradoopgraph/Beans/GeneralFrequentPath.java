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
    private List<Integer> edgeId;
    private String endNodeLabel;

    public GeneralFrequentPath(String startNodeLabel, List<Integer> edgeId, String endNodeLabel) {
        this.startNodeLabel = startNodeLabel;
        this.edgeId = edgeId;
        this.endNodeLabel = endNodeLabel;
    }

    public String getStartNodeLabel() {
        return startNodeLabel;
    }

    public void setStartNodeLabel(String startNodeLabel) {
        this.startNodeLabel = startNodeLabel;
    }

    public List<Integer> getEdgeId() {
        return edgeId;
    }

    public void setEdgeId(List<Integer> edgeId) {
        this.edgeId = edgeId;
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
                ", edgeId=" + edgeId +
                ", endNodeLabel='" + endNodeLabel + '\'' +
                '}';
    }
}
