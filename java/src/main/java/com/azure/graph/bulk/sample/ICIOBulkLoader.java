// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.graph.bulk.sample;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.graph.bulk.impl.BulkGremlinObjectMapper;
import com.azure.graph.bulk.impl.GremlinDocumentOperationCreator;
import com.azure.graph.bulk.sample.model.CountrySectorVertex;
import com.azure.graph.bulk.sample.model.SupplyEdge;
import com.azure.graph.bulk.sample.model.ICIOGraphData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.stream.Stream;

/**
 * Bulk loader for ICIO (Inter-Country Input-Output) graph data into Azure Cosmos DB.
 * This class handles the bulk insertion of country-sector vertices and supply relationship edges.
 */
public class ICIOBulkLoader {
    private static final Logger log = LoggerFactory.getLogger(ICIOBulkLoader.class);
    
    private final CosmosAsyncClient cosmosClient;
    private final CosmosAsyncDatabase database;
    private final CosmosAsyncContainer container;
    private final GremlinDocumentOperationCreator documentOperationCreator;
    
    public ICIOBulkLoader(String connectionString, String databaseName, String containerName, int throughput) {
        this.cosmosClient = new CosmosClientBuilder()
                .endpoint(connectionString)
                .key(DatabaseSettings.MASTER_KEY)
                .buildAsyncClient();
        
        // Create database if it doesn't exist
        this.cosmosClient.createDatabaseIfNotExists(databaseName).block();
        this.database = this.cosmosClient.getDatabase(databaseName);
        
        // Create container if it doesn't exist with partition key "pk"
        this.database.createContainerIfNotExists(
                containerName, 
                "/pk", 
                ThroughputProperties.createManualThroughput(throughput)
        ).block();
        this.container = this.database.getContainer(containerName);
        
        // Initialize document operation creator
        this.documentOperationCreator = GremlinDocumentOperationCreator.builder()
                .mapper(BulkGremlinObjectMapper.getBulkGremlinObjectMapper())
                .build();
        
        log.info("ICIO Bulk Loader initialized for database: {}, container: {}, throughput: {}", 
                databaseName, containerName, throughput);
    }
    
    /**
     * Uploads ICIO graph data using bulk operations.
     * 
     * @param graphData The ICIO graph data to upload
     * @param createDocs Whether to use create operations (true) or upsert operations (false)
     */
    public void uploadICIOGraphData(ICIOGraphData graphData, boolean createDocs) {
        log.info("Starting bulk upload of ICIO graph data: {} vertices, {} edges", 
                graphData.getVertexCount(), graphData.getEdgeCount());
        
        Stream<CountrySectorVertex> vertexStream = graphData.getVertices().stream();
        Stream<SupplyEdge> edgeStream = graphData.getEdges().stream();
        
        uploadDocuments(vertexStream, edgeStream, createDocs);
        
        log.info("Completed bulk upload of ICIO graph data");
    }
    
    /**
     * Uploads vertices and edges using bulk operations.
     * 
     * @param vertices Stream of CountrySectorVertex objects
     * @param edges Stream of SupplyEdge objects
     * @param createDocs Whether to use create operations (true) or upsert operations (false)
     */
    public void uploadDocuments(Stream<CountrySectorVertex> vertices, Stream<SupplyEdge> edges, boolean createDocs) {
        Stream<Object> vertexObjectStream = vertices.map(v -> (Object) v);
        Stream<Object> edgeObjectStream = edges.map(e -> (Object) e);
        
        Stream<com.azure.cosmos.models.CosmosItemOperation> operations;
        if (createDocs) {
            operations = documentOperationCreator.getVertexCreateOperations(vertexObjectStream);
            operations = Stream.concat(operations, documentOperationCreator.getEdgeCreateOperations(edgeObjectStream));
        } else {
            operations = documentOperationCreator.getVertexUpsertOperations(vertexObjectStream);
            operations = Stream.concat(operations, documentOperationCreator.getEdgeUpsertOperations(edgeObjectStream));
        }
        
        container.executeBulkOperations(Flux.fromStream(operations))
                .filter(r -> r.getException() != null || r.getResponse().getStatusCode() > 299)
                .doOnNext(r -> {
                    if (r.getException() != null) {
                        log.error("Failed with exception: {}", r.getException().getMessage());
                    } else {
                        log.error("Failed with status code: {}", r.getResponse().getStatusCode());
                    }
                })
                .blockLast();
    }
    
    /**
     * Closes the Cosmos client connection.
     */
    public void close() {
        if (cosmosClient != null) {
            cosmosClient.close();
        }
    }
}