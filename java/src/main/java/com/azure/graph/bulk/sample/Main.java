// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.graph.bulk.sample;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import com.azure.graph.bulk.sample.model.CountrySectorVertex;
import com.azure.graph.bulk.sample.model.SupplyEdge;
import com.azure.graph.bulk.impl.model.GremlinEdgeVertexInfo;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

/**
 * Main class for demonstrating ICIO (Inter-Country Input-Output) graph bulk loading.
 * This class provides command-line interface for loading ICIO data into Azure Cosmos DB.
 */
public class Main {
    private static final String CONNECTION_STRING = "https://" + DatabaseSettings.HOST + ".documents.azure.com:443/";
    private static final String DATABASE_NAME = DatabaseSettings.DATABASE_NAME;
    private static final String CONTAINER_NAME = DatabaseSettings.CONTAINER_NAME;
    private static final int DEFAULT_THROUGHPUT = DatabaseSettings.THROUGHPUT;
    
    public static void main(String[] args) {
        try {
            Options options = getOptions();
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);
            
            // Require CSV file
            String csvFile = cmd.getOptionValue(ArgNames.CSV_FILE);
            if (csvFile == null || csvFile.trim().isEmpty()) {
                System.err.println("Error: CSV file is required. Use -f <file> to specify the CSV file path.");
                System.err.println("Run with --help to see all options.");
                System.exit(1);
            }
            
            // Parse command line arguments
            double minValueThreshold = Double.parseDouble(cmd.getOptionValue(ArgNames.MIN_VALUE, "1.0"));
            boolean createDocs = cmd.hasOption(ArgNames.CREATE_DOCS);
            int throughput = Integer.parseInt(cmd.getOptionValue(ArgNames.THROUGHPUT, String.valueOf(DEFAULT_THROUGHPUT)));
            
            System.out.println("ICIO Graph Bulk Loader");
            System.out.println("======================");
            System.out.println("CSV file: " + csvFile);
            System.out.println("Min value threshold: " + minValueThreshold);
            System.out.println("Operation type: " + (createDocs ? "CREATE" : "UPSERT"));
            System.out.println("Throughput: " + throughput);
            System.out.println();
            
            // Initialize bulk loader
            ICIOBulkLoader loader = new ICIOBulkLoader(CONNECTION_STRING, DATABASE_NAME, CONTAINER_NAME, throughput);
            
            try {
                // Use streaming approach to avoid OutOfMemoryError with large datasets
                streamingUploadICIOGraph(csvFile, minValueThreshold, loader, createDocs);
                System.out.println("Successfully uploaded ICIO data from CSV to Cosmos DB!");
            } finally {
                loader.close();
            }
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private static Options getOptions() {
        Options options = new Options();
        
        options.addOption(
                "f",
                ArgNames.CSV_FILE,
                true,
                "CSV file path containing ICIO data (required)");
       
        options.addOption(
                "m",
                ArgNames.MIN_VALUE,
                true,
                "Minimum value threshold for edges (default: 1.0)");
        options.addOption(
                "c",
                ArgNames.CREATE_DOCS,
                false,
                "Use create operations instead of upsert operations");
        options.addOption(
                "t",
                ArgNames.THROUGHPUT,
                true,
                "Container throughput in RU/s (default: 1000)");
        
        return options;
    }
    
    /**
     * Loads and uploads ICIO data from CSV using streaming approach to avoid memory issues.
     * This method processes data in batches instead of loading everything into memory.
     */
    private static void streamingUploadICIOGraph(String csvFilePath, double minValueThreshold, 
                                                         ICIOBulkLoader loader, boolean createDocs) throws IOException {
        System.out.println("Using streaming approach to process large dataset...");
        
        // First pass: collect row and column labels
        List<String> rowLabels = new ArrayList<>();
        List<String> colLabels = new ArrayList<>();
        int rowCount = 0;
        
        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
            String line = br.readLine(); // Header row
            if (line != null) {
                String[] headers = line.split(",");
                for (int i = 1; i < headers.length; i++) {
                    String colLabel = headers[i].trim();
                    if (colLabel.contains("_")) {
                        colLabels.add(colLabel);
                    }
                }
            }
            
            // Count rows and collect row labels
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                if (values.length > 0) {
                    String firstColumn = values[0].trim();
                    if (firstColumn.contains("_")) {
                        rowLabels.add(firstColumn);
                        rowCount++;
                    }
                }
            }
        }
        
        System.out.println("Found " + rowCount + " supplier sectors and " + colLabels.size() + " consumer sectors");
        System.out.println("Total potential edges: " + (rowCount * colLabels.size()));
        
        // Create all vertices first (this is small - only ~4K vertices)
        Set<String> allSectors = new HashSet<>(rowLabels);
        allSectors.addAll(colLabels);
        System.out.println("Creating vertices for " + allSectors.size() + " unique sectors...");
        
        List<CountrySectorVertex> vertices = new ArrayList<>();
        for (String sectorId : allSectors) {
            if (!sectorId.contains("_")) {
                System.err.println("ERROR: Invalid sector ID found: '" + sectorId + "' - skipping");
                continue;
            }
            vertices.add(CountrySectorVertex.fromCountrySectorId(sectorId));
        }
        
        // Create a map for quick vertex lookup
        Map<String, CountrySectorVertex> vertexMap = vertices.stream()
                .collect(java.util.stream.Collectors.toMap(CountrySectorVertex::getId, v -> v));
        
        System.out.println("Created " + vertices.size() + " vertices");
        
        // Upload vertices first
        System.out.println("Uploading vertices...");
        System.out.println("Using operation type: " + (createDocs ? "CREATE" : "UPSERT"));
        loader.uploadDocuments(vertices.stream(), Stream.empty(), createDocs);
        
        // Process edges in batches to avoid memory issues
        System.out.println("Processing edges in batches...");
        processEdgesInBatches(csvFilePath, vertexMap, colLabels, minValueThreshold, loader, createDocs);
    }
    
    /**
     * Processes edges in batches to avoid loading all edges into memory at once.
     */
    private static void processEdgesInBatches(String csvFilePath, Map<String, CountrySectorVertex> vertexMap,
                                            List<String> colLabels, double minValueThreshold,
                                            ICIOBulkLoader loader, boolean createDocs) throws IOException {
        final int BATCH_SIZE = 10000; // Process 10K edges at a time
        List<SupplyEdge> edgeBatch = new ArrayList<>();
        int totalEdgesProcessed = 0;
        
        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
            String line = br.readLine(); // Skip header row
            
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                if (values.length > 1) {
                    String sourceSector = values[0].trim();
                    
                    if (sourceSector.contains("_")) {
                        CountrySectorVertex sourceVertex = vertexMap.get(sourceSector);
                        if (sourceVertex != null) {
                            // Process each column (consumer sector)
                            for (int col = 1; col < values.length && col - 1 < colLabels.size(); col++) {
                                try {
                                    double value = Double.parseDouble(values[col].trim());
                                    if (value > minValueThreshold) {
                                        String targetSector = colLabels.get(col - 1);
                                        if (targetSector.contains("_")) {
                                            CountrySectorVertex targetVertex = vertexMap.get(targetSector);
                                            if (targetVertex != null) {
                                                edgeBatch.add(SupplyEdge.builder()
                                                        .sourceVertexInfo(GremlinEdgeVertexInfo.fromGremlinVertex(sourceVertex))
                                                        .destinationVertexInfo(GremlinEdgeVertexInfo.fromGremlinVertex(targetVertex))
                                                        .label("million_USD")
                                                        .value(value)
                                                        .build());
                                                
                                                // Upload batch when it reaches the batch size
                                                if (edgeBatch.size() >= BATCH_SIZE) {
                                                    System.out.println("Uploading batch of " + edgeBatch.size() + " edges...");
                                                    loader.uploadDocuments(Stream.empty(), edgeBatch.stream(), createDocs);
                                                    totalEdgesProcessed += edgeBatch.size();
                                                    System.out.println("Total edges processed: " + totalEdgesProcessed);
                                                    edgeBatch.clear();
                                                }
                                            }
                                        }
                                    }
                                } catch (NumberFormatException e) {
                                    // Skip invalid values
                                }
                            }
                        }
                    }
                }
            }
            
            // Upload remaining edges in the last batch
            if (!edgeBatch.isEmpty()) {
                System.out.println("Uploading final batch of " + edgeBatch.size() + " edges...");
                loader.uploadDocuments(Stream.empty(), edgeBatch.stream(), createDocs);
                totalEdgesProcessed += edgeBatch.size();
            }
        }
        
        System.out.println("Total edges processed: " + totalEdgesProcessed);
    }

}