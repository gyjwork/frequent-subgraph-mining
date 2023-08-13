package com.ncl.gradoopgraph.TestFunction;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class GraphDataGenerator {

    enum CarStatus {
        FOR_SALE,
        OWNED,
        SOLD
    }

    public static void main(String[] args) throws IOException {
        int numPersons = 5;
        int numCars = 5;
        int numTransactions = 20;
        String directoryPath = "src/main/resources/myfolder——small";
        File directory = new File(directoryPath);
        if (!directory.exists()) {
            directory.mkdirs();
        }
        generateGraphData(numPersons, numCars, numTransactions, directoryPath);
    }

    private static void generateGraphData(int numPersons, int numCars, int numTransactions, String directoryPath) throws IOException {
        Random random = new Random();
        long time = System.currentTimeMillis();
        CarStatus[] carStatus = new CarStatus[numCars];
        int[] carOwners = new int[numCars];
        Map<Integer, Integer> ownsEdges = new HashMap<>();

        for (int i = 0; i < numCars; i++) {
            carStatus[i] = CarStatus.FOR_SALE;
            carOwners[i] = -1;
        }

        try (FileWriter verticesWriter = new FileWriter(directoryPath + "/vertices.csv");
             FileWriter edgesWriter = new FileWriter(directoryPath + "/edges.csv")) {

            // Generate persons and cars
            for (int i = 0; i < numPersons; i++) {
                verticesWriter.write(String.format("%024d", i) + ";[000000000000000000000000];person;person" + (i + 1) + "\n");
            }
            for (int i = 0; i < numCars; i++) {
                verticesWriter.write(String.format("%024d", numPersons + i) + ";[000000000000000000000000];car;car" + (i + 1) + "\n");
            }

            int edgeId = 0;
            int buyerIndex, sellerIndex;
            int carIndex;

            // Generate edges (transactions)
            for (int i = 0; i < numTransactions; i++) {
                List<Integer> availableCars = new ArrayList<>();

                for (int j = 0; j < numCars; j++) {
                    if (carStatus[j] == CarStatus.FOR_SALE) {
                        availableCars.add(j);
                    }
                }

                if (availableCars.isEmpty()) {
                    break;
                }

                carIndex = availableCars.get(random.nextInt(availableCars.size()));
                sellerIndex = carOwners[carIndex];

                do {
                    buyerIndex = random.nextInt(numPersons);
                } while (buyerIndex == sellerIndex);

                // Buys
                edgesWriter.write(String.format("%024d", edgeId++) + ";[000000000000000000000000];" +
                        String.format("%024d", buyerIndex) + ";" +
                        String.format("%024d", numPersons + carIndex) + ";buys;" + time + "\n");
                time += 86400000;

                // Owns
                int ownsEdgeId = edgeId++;
                ownsEdges.put(carIndex, ownsEdgeId);
                edgesWriter.write(String.format("%024d", ownsEdgeId) + ";[000000000000000000000000];" +
                        String.format("%024d", buyerIndex) + ";" +
                        String.format("%024d", numPersons + carIndex) + ";owns;" + time + "\n");

                carStatus[carIndex] = CarStatus.OWNED;
                carOwners[carIndex] = buyerIndex;
                time += 86400000;

                // Check if car is sold
                if (random.nextBoolean() && carStatus[carIndex] == CarStatus.OWNED) {
                    // Sells
                    edgesWriter.write(String.format("%024d", edgeId++) + ";[000000000000000000000000];" +
                            String.format("%024d", buyerIndex) + ";" +
                            String.format("%024d", numPersons + carIndex) + ";sells;" + time + "\n");

                    // Update the owns edge with end time
                    edgesWriter.write(String.format("%024d", ownsEdgeId) + ";[000000000000000000000000];" +
                            String.format("%024d", buyerIndex) + ";" +
                            String.format("%024d", numPersons + carIndex) + ";owns;" + (time - 86400000) + "|" + time + "\n");

                    carStatus[carIndex] = CarStatus.SOLD;
                }

                // Check if car is sold and bought by another person
                if (carStatus[carIndex] == CarStatus.SOLD) {
                    int newBuyerIndex;
                    do {
                        newBuyerIndex = random.nextInt(numPersons);
                    } while (newBuyerIndex == buyerIndex);

                    // Buys
                    edgesWriter.write(String.format("%024d", edgeId++) + ";[000000000000000000000000];" +
                            String.format("%024d", newBuyerIndex) + ";" +
                            String.format("%024d", numPersons + carIndex) + ";buys;" + time + "\n");
                    time += 86400000; // Time of the buy

                    // Transfers ownership
                    edgesWriter.write(String.format("%024d", edgeId++) + ";[000000000000000000000000];" +
                            String.format("%024d", buyerIndex) + ";" +
                            String.format("%024d", newBuyerIndex) + ";transfers_ownership;car" + (carIndex + 1) + "|" + time + "\n");
                    // Time stays the same for ownership transfer

                    // Owns (New owner)
                    int newOwnsEdgeId = edgeId++;
                    edgesWriter.write(String.format("%024d", newOwnsEdgeId) + ";[000000000000000000000000];" +
                            String.format("%024d", newBuyerIndex) + ";" +
                            String.format("%024d", numPersons + carIndex) + ";owns;" + time + "\n"); // Same time as ownership transfer

                    // Update the ownsEdges map to reflect the new ownership edge
                    ownsEdges.put(carIndex, newOwnsEdgeId);
                }
            }
        }
    }
}
