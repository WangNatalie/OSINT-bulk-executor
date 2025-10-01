// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.graph.bulk.impl;

import com.azure.graph.bulk.impl.annotations.GremlinId;
import com.azure.graph.bulk.impl.annotations.GremlinLabel;
import com.azure.graph.bulk.impl.annotations.GremlinLabelGetter;
import com.azure.graph.bulk.impl.model.AnnotationValidationException;
import com.azure.graph.bulk.impl.model.GremlinPartitionKey;
import com.azure.graph.bulk.impl.model.GremlinVertex;
import com.azure.graph.bulk.sample.model.CountrySectorVertex;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ObjectToVertexTest {

    @Test
    void CountrySectorVertexToGremlinVertexTest() {
        CountrySectorVertex source = getCountrySectorVertex();
        GremlinVertex converted = ObjectToVertex.toGremlinVertex(source);

        assertEquals("Country_sector", converted.getLabel());
        assertEquals(source.id, converted.getId());

        validatePartitionKey(source, converted);
        assertTrue(converted.getProperties().containsKey("country"));
        assertTrue(converted.getProperties().containsKey("sector"));
        assertTrue(converted.getProperties().containsKey("id_str"));

        assertFalse(converted.getProperties().containsKey("pk"));

    }

    private void validatePartitionKey(CountrySectorVertex source, GremlinVertex converted) {
        GremlinPartitionKey partitionKey = converted.getPartitionKey();

        assertNotNull(partitionKey);
        assertEquals("pk", partitionKey.getFieldName());
        assertEquals(source.pk, partitionKey.getValue());
    }

    private CountrySectorVertex getCountrySectorVertex() {

        return CountrySectorVertex.builder()
                .id("USA_MANUFACTURING")
                .pk("USA_MANUFACTURING")
                .country("USA")
                .sector("MANUFACTURING")
                .idStr("USA_MANUFACTURING")
                .build();
    }

    @com.azure.graph.bulk.impl.annotations.GremlinVertex(label = "TheLabel")
    static class SoManyProblems {
        @GremlinId
        public String id;
        @GremlinId
        public String id2;
        @com.azure.graph.bulk.impl.annotations.GremlinPartitionKey
        public String partitionKey;
        @com.azure.graph.bulk.impl.annotations.GremlinPartitionKey
        public String partitionKey2;
        @GremlinLabel
        public String label;

        @GremlinLabelGetter
        public String label() {
            return "TheLabel";
        }
    }

    @Test
    void SoManyProblemsThrowsException() {
        SoManyProblems problems = new SoManyProblems();
        assertThrows(AnnotationValidationException.class, () -> ObjectToVertex.toGremlinVertex(problems));
    }
}
