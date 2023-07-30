package com.ncl.gradoopgraph.Beans;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author gyj
 * @title: GeneralFrequentPath
 * @projectName gradoop-graph
 * @description: TODO
 * @date 7/17/23 3:27 PM
 */
public class GeneralFrequentPath {
    private String startNodeLabel;
    private Set<Integer> edgeIds;
    private String endNodeLabel;

    public GeneralFrequentPath(String startNodeLabel, Set<Integer> edgeIds, String endNodeLabel) {
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

    public Set<Integer> getEdgeIds() {
        return edgeIds;
    }

    public void setEdgeIds(Set<Integer> edgeIds) {
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

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof GeneralFrequentPath)) {
            return false;
        }
        GeneralFrequentPath other = (GeneralFrequentPath) obj;
        return startNodeLabel.equals(other.startNodeLabel) && endNodeLabel.equals(other.endNodeLabel);
    }

    @Override
    public int hashCode() {
        int result = startNodeLabel.hashCode();
        result = 31 * result + endNodeLabel.hashCode();
        return result;
    }

    public GeneralFrequentPath merge(GeneralFrequentPath other) {
        if (!this.equals(other)) {
            throw new IllegalArgumentException("Cannot merge paths with different start or end labels.");
        }
        Set<Integer> mergedIds = new HashSet<>(this.edgeIds);
        mergedIds.addAll(other.edgeIds);
        return new GeneralFrequentPath(this.startNodeLabel, mergedIds, this.endNodeLabel);
    }
}
