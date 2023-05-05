package com.ncl.gradoopgraph.utils;

/**
 * @author gyj
 * @title: EdgeTypeInfo
 * @projectName gradoop-graph
 * @description: TODO
 * @date 5/4/236:07 PM
 */
public class EdgeTypeInfo {
    private int priority;
    private boolean isDirected;

    public EdgeTypeInfo(int priority, boolean isDirected) {
        this.priority = priority;
        this.isDirected = isDirected;
    }

    public int getPriority() {
        return priority;
    }

    public boolean isDirected() {
        return isDirected;
    }
}

