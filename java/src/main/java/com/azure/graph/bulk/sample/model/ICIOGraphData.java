// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.graph.bulk.sample.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import com.azure.graph.bulk.impl.model.GremlinEdgeVertexInfo;

/**
 * Represents Input-Output table data for bulk loading into Cosmos DB graph.
 * This class encapsulates the vertices (country-sectors) and edges (supply relationships)
 * parsed from an ICIO CSV file.
 */
public class ICIOGraphData {
    private final List<CountrySectorVertex> vertices;
    private final List<SupplyEdge> edges;
    private final int totalRows;
    private final int totalColumns;
    private final double minValueThreshold;

    ICIOGraphData(ICIOGraphDataBuilder builder) {
        this.vertices = builder.vertices;
        this.edges = builder.edges;
        this.totalRows = builder.totalRows;
        this.totalColumns = builder.totalColumns;
        this.minValueThreshold = builder.minValueThreshold;
    }

    public static ICIOGraphDataBuilder builder() {
        return new ICIOGraphDataBuilder();
    }

    /**
     * Creates ICIOGraphData from CSV data represented as a matrix.
     * 
     * @param dataMatrix 2D array where [row][col] represents supply from row country-sector to col country-sector
     * @param rowLabels Country-sector IDs for rows (suppliers)
     * @param colLabels Country-sector IDs for columns (consumers)
     * @param minValueThreshold Minimum value threshold for including edges
     * @return ICIOGraphData instance
     */
    public static ICIOGraphData fromMatrix(double[][] dataMatrix, 
                                         String[] rowLabels, 
                                         String[] colLabels, 
                                         double minValueThreshold) {
        if (dataMatrix == null || rowLabels == null || colLabels == null) {
            throw new IllegalArgumentException("Data matrix and labels cannot be null");
        }
        if (dataMatrix.length != rowLabels.length) {
            throw new IllegalArgumentException("Data matrix rows must match row labels length");
        }
        if (dataMatrix.length > 0 && dataMatrix[0].length != colLabels.length) {
            throw new IllegalArgumentException("Data matrix columns must match column labels length");
        }

        ICIOGraphDataBuilder builder = ICIOGraphData.builder()
                .totalRows(rowLabels.length)
                .totalColumns(colLabels.length)
                .minValueThreshold(minValueThreshold);

        // Collect all unique country-sector IDs
        Set<String> allCountrySectors = new HashSet<>();
        for (String label : rowLabels) {
            allCountrySectors.add(label);
        }
        for (String label : colLabels) {
            allCountrySectors.add(label);
        }

        // Create vertices for all country-sectors
        for (String countrySectorId : allCountrySectors) {
            builder.addVertex(CountrySectorVertex.fromCountrySectorId(countrySectorId));
        }

        // Create a map for quick vertex lookup
        java.util.Map<String, CountrySectorVertex> vertexMap = new java.util.HashMap<>();
        for (CountrySectorVertex vertex : builder.vertices) {
            vertexMap.put(vertex.getId(), vertex);
        }

        // Create edges for non-zero values above threshold
        for (int i = 0; i < rowLabels.length; i++) {
            for (int j = 0; j < colLabels.length; j++) {
                double value = dataMatrix[i][j];
                if (value > minValueThreshold) {
                    String sourceId = rowLabels[i];
                    String destinationId = colLabels[j];
                    CountrySectorVertex sourceVertex = vertexMap.get(sourceId);
                    CountrySectorVertex destinationVertex = vertexMap.get(destinationId);
                    if (sourceVertex != null && destinationVertex != null) {
                        builder.addEdge(SupplyEdge.builder()
                                .sourceVertexInfo(GremlinEdgeVertexInfo.fromGremlinVertex(sourceVertex))
                                .destinationVertexInfo(GremlinEdgeVertexInfo.fromGremlinVertex(destinationVertex))
                                .label("million_USD")
                                .value(value)
                                .build());
                    }
                }
            }
        }

        return builder.build();
    }

    public List<CountrySectorVertex> getVertices() {
        return vertices;
    }

    public List<SupplyEdge> getEdges() {
        return edges;
    }

    public int getTotalRows() {
        return totalRows;
    }

    public int getTotalColumns() {
        return totalColumns;
    }

    public double getMinValueThreshold() {
        return minValueThreshold;
    }

    public int getVertexCount() {
        return vertices.size();
    }

    public int getEdgeCount() {
        return edges.size();
    }

    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof ICIOGraphData)) return false;
        ICIOGraphData other = (ICIOGraphData) o;

        if (isNotEqual(vertices, other.vertices)) return false;
        if (isNotEqual(edges, other.edges)) return false;
        if (totalRows != other.totalRows) return false;
        if (totalColumns != other.totalColumns) return false;
        //noinspection RedundantIfStatement
        if (Double.compare(minValueThreshold, other.minValueThreshold) != 0) return false;

        return true;
    }

    private boolean isNotEqual(Object source, Object other) {
        if (source == null && other == null) return false;
        if (source == null) return true;
        return !source.equals(other);
    }

    public int hashCode() {
        int result = 59 + (vertices == null ? 43 : vertices.hashCode());
        result = result * 59 + (edges == null ? 43 : edges.hashCode());
        result = result * 59 + totalRows;
        result = result * 59 + totalColumns;
        result = result * 59 + Double.hashCode(minValueThreshold);
        return result;
    }

    public static class ICIOGraphDataBuilder {
        private List<CountrySectorVertex> vertices;
        private List<SupplyEdge> edges;
        private int totalRows;
        private int totalColumns;
        private double minValueThreshold;

        ICIOGraphDataBuilder() {
            this.vertices = new ArrayList<>();
            this.edges = new ArrayList<>();
        }

        public ICIOGraphDataBuilder addVertex(CountrySectorVertex vertex) {
            this.vertices.add(vertex);
            return this;
        }

        public ICIOGraphDataBuilder addEdge(SupplyEdge edge) {
            this.edges.add(edge);
            return this;
        }

        public ICIOGraphDataBuilder vertices(List<CountrySectorVertex> vertices) {
            this.vertices = vertices;
            return this;
        }

        public ICIOGraphDataBuilder edges(List<SupplyEdge> edges) {
            this.edges = edges;
            return this;
        }

        public ICIOGraphDataBuilder totalRows(int totalRows) {
            this.totalRows = totalRows;
            return this;
        }

        public ICIOGraphDataBuilder totalColumns(int totalColumns) {
            this.totalColumns = totalColumns;
            return this;
        }

        public ICIOGraphDataBuilder minValueThreshold(double minValueThreshold) {
            this.minValueThreshold = minValueThreshold;
            return this;
        }

        public ICIOGraphData build() {
            return new ICIOGraphData(this);
        }
    }
}
