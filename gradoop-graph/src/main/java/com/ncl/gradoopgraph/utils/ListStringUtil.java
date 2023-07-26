package com.ncl.gradoopgraph.utils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
/**
 * @author gyj
 * @title: ListStringUtil
 * @projectName gradoop-graph
 * @description: TODO
 * @date 7/26/233:25 AM
 */

public class ListStringUtil {

    // Method to convert List<Integer> to String
    public static String listToString(List<Integer> list) {
        return list.toString();
    }

    // Method to convert String to List<Integer>
    public static List<Integer> stringToList(String str) {
        return Arrays.stream(str.substring(1, str.length() - 1).split(","))
                .map(String::trim).map(Integer::parseInt).collect(Collectors.toList());
    }
}

