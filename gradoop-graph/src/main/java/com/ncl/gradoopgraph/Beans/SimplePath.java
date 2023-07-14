package com.ncl.gradoopgraph.Beans;

import java.util.Objects;

/**
 * @author gyj
 * @title: SimplePath
 * @projectName gradoop-graph
 * @description: TODO
 * @date 7/13/236:44 PM
 */
public class SimplePath {
    private String startNode;
    private String endNode;

    public SimplePath(String startNode, String endNode) {
        this.startNode = startNode;
        this.endNode = endNode;
    }

    public String getStartNode() {
        return startNode;
    }

    public String getEndNode() {
        return endNode;
    }

    @Override
    public String toString() {
        return startNode + "~" + endNode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimplePath that = (SimplePath) o;
        return Objects.equals(startNode, that.startNode) &&
                Objects.equals(endNode, that.endNode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startNode, endNode);
    }
}

