// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.graph.bulk.impl;

import com.azure.graph.bulk.impl.annotations.GremlinId;
import com.azure.graph.bulk.impl.model.AnnotationValidationException;
import com.azure.graph.bulk.impl.model.GremlinEdge;
import com.azure.graph.bulk.impl.model.GremlinEdgeVertexInfo;
import com.azure.graph.bulk.impl.model.GremlinPartitionKey;
import com.azure.graph.bulk.sample.model.SupplyEdge;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ObjectToEdgeTest {

    @Test
    void SupplyEdgeToGremlinEdgeTest() {
        SupplyEdge edge = getSupplyEdge();
        GremlinEdge results = ObjectToEdge.toGremlinEdge(edge);

        assertEquals(edge.destinationVertexInfo.getId(), results.getDestinationVertexInfo().getId());
        assertEquals(edge.sourceVertexInfo.getId(), results.getSourceVertexInfo().getId());
        assertEquals(edge.destinationVertexInfo.getPartitionKey(),
                results.getDestinationVertexInfo().getPartitionKey());
        assertEquals(edge.sourceVertexInfo.getPartitionKey(), results.getSourceVertexInfo().getPartitionKey());
        assertEquals(edge.destinationVertexInfo.getLabel(), results.getDestinationVertexInfo().getLabel());
        assertEquals(edge.sourceVertexInfo.getLabel(), results.getSourceVertexInfo().getLabel());

        assertNotNull(results.getId());

    }

    private SupplyEdge getSupplyEdge() {
        SupplyEdge edge = SupplyEdge.builder()
                .sourceVertexInfo(GremlinEdgeVertexInfo.builder()
                        .id("USA_MANUFACTURING")
                        .label("Country_sector")
                        .partitionKey(GremlinPartitionKey.builder().value("USA_MANUFACTURING").build()).build())
                .destinationVertexInfo(GremlinEdgeVertexInfo.builder()
                        .id("CHINA_SERVICES")
                        .label("Country_sector")
                        .partitionKey(GremlinPartitionKey.builder().value("CHINA_SERVICES").build()).build())
                .label("supplies")
                .value(1000000.0)
                .build();

        return edge;
    }

    @com.azure.graph.bulk.impl.annotations.GremlinEdge(partitionKeyFieldName = "pk-field")
    static class SoManyProblems {
        @GremlinId
        public String id;
        @GremlinId
        public String id2;
        @com.azure.graph.bulk.impl.annotations.GremlinPartitionKey
        public String partitionKey;
    }

    @Test
    void SoManyProblemsThrowsException() {
        SoManyProblems problems = new SoManyProblems();
        assertThrows(AnnotationValidationException.class, () -> ObjectToEdge.toGremlinEdge(problems));
    }
}
