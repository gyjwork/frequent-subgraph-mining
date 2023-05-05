package com.ncl.gradoopgraph.utils;

import org.gradoop.common.model.impl.pojo.EPGMVertex;

/**
 * @author gyj
 * @title: NewVertex
 * @projectName gradoop-graph
 * @description: TODO
 * @date 5/2/233:16 AM
 */
public class NewVertex implements Comparable<NewVertex> {

    public EPGMVertex vertex;
    public long timestamp;
    public int priority;

    public NewVertex(EPGMVertex vertex, long timestamp, int priority) {
        this.vertex = vertex;
        this.timestamp = timestamp;
        this.priority = priority;
    }

    @Override
    public int compareTo(NewVertex other) {
        if (this.timestamp != other.timestamp) {
            return Long.compare(this.timestamp, other.timestamp);
        } else {
            return Integer.compare(this.priority, other.priority);
        }
    }

    public EPGMVertex getVertex() {
        return vertex;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getPriority() {
        return priority;
    }
}


