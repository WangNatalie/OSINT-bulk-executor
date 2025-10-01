// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.graph.bulk.sample.model;

import com.azure.graph.bulk.impl.annotations.GremlinEdge;
import com.azure.graph.bulk.impl.annotations.GremlinEdgeVertex;
import com.azure.graph.bulk.impl.annotations.GremlinEdgeVertex.Direction;
import com.azure.graph.bulk.impl.annotations.GremlinLabel;
import com.azure.graph.bulk.impl.annotations.GremlinProperty;
import com.azure.graph.bulk.impl.model.GremlinEdgeVertexInfo;

/**
 * Represents a supply relationship edge in an Input-Output table graph.
 * Each edge represents a monetary flow from one country-sector to another.
 * 
 * Example: USA_MANUFACTURING supplies CHINA_SERVICES with a value of $1,000,000
 */
@GremlinEdge(partitionKeyFieldName = "pk")
public class SupplyEdge {
    @GremlinEdgeVertex(direction = Direction.DESTINATION)
    public GremlinEdgeVertexInfo destinationVertexInfo;
    
    @GremlinEdgeVertex(direction = Direction.SOURCE)
    public GremlinEdgeVertexInfo sourceVertexInfo;
    
    @GremlinLabel
    public String label;
    
    @GremlinProperty(name = "value")
    public Double value;

    SupplyEdge(SupplyEdge.SupplyEdgeBuilder builder) {
        destinationVertexInfo = builder.destinationVertexInfo;
        sourceVertexInfo = builder.sourceVertexInfo;
        label = builder.label;
        value = builder.value;
    }

    public static SupplyEdge.SupplyEdgeBuilder builder() {
        return new SupplyEdge.SupplyEdgeBuilder();
    }

    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof SupplyEdge)) return false;
        SupplyEdge other = (SupplyEdge) o;

        if (isNotEqual(destinationVertexInfo, other.destinationVertexInfo)) return false;
        if (isNotEqual(sourceVertexInfo, other.sourceVertexInfo)) return false;
        if (isNotEqual(label, other.label)) return false;
        //noinspection RedundantIfStatement
        if (isNotEqual(value, other.value)) return false;

        return true;
    }

    private boolean isNotEqual(Object source, Object other) {
        if (source == null && other == null) return false;
        if (source == null) return true;
        return !source.equals(other);
    }

    public int hashCode() {
        int result = 59 + (destinationVertexInfo == null ? 43 : destinationVertexInfo.hashCode());
        result = result * 59 + (sourceVertexInfo == null ? 43 : sourceVertexInfo.hashCode());
        result = result * 59 + (label == null ? 43 : label.hashCode());
        result = result * 59 + (value == null ? 43 : value.hashCode());
        return result;
    }

    public static class SupplyEdgeBuilder {
        private GremlinEdgeVertexInfo destinationVertexInfo;
        private GremlinEdgeVertexInfo sourceVertexInfo;
        private String label;
        private Double value;

        SupplyEdgeBuilder() {
        }

        public SupplyEdge.SupplyEdgeBuilder destinationVertexInfo(GremlinEdgeVertexInfo destinationVertexInfo) {
            this.destinationVertexInfo = destinationVertexInfo;
            return this;
        }

        public SupplyEdge.SupplyEdgeBuilder sourceVertexInfo(GremlinEdgeVertexInfo sourceVertexInfo) {
            this.sourceVertexInfo = sourceVertexInfo;
            return this;
        }

        public SupplyEdge.SupplyEdgeBuilder label(String label) {
            this.label = label;
            return this;
        }

        public SupplyEdge.SupplyEdgeBuilder value(Double value) {
            this.value = value;
            return this;
        }

        public SupplyEdge build() {
            return new SupplyEdge(this);
        }
    }
}
