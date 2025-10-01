// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.graph.bulk.sample.model;

import com.azure.graph.bulk.impl.annotations.GremlinId;
import com.azure.graph.bulk.impl.annotations.GremlinPartitionKey;
import com.azure.graph.bulk.impl.annotations.GremlinProperty;
import com.azure.graph.bulk.impl.annotations.GremlinVertex;

/**
 * Represents a country-sector vertex in an Input-Output table graph.
 * Each vertex represents a specific industry sector within a country.
 * 
 * Example: "USA_MANUFACTURING" represents the manufacturing sector in the USA.
 */
@GremlinVertex(label = "Country_sector")
public class CountrySectorVertex {
    @GremlinId
    public String id;
    
    @GremlinPartitionKey
    public String pk;
    
    @GremlinProperty(name = "country")
    public String country;
    
    @GremlinProperty(name = "sector")
    public String sector;
    
    @GremlinProperty(name = "id_str") // TODO: Remove this field and use id solely
    public String idStr;

    CountrySectorVertex(CountrySectorVertex.CountrySectorVertexBuilder builder) {
        this.id = builder.id;
        this.pk = builder.pk;
        this.country = builder.country;
        this.sector = builder.sector;
        this.idStr = builder.idStr;
    }

    public static CountrySectorVertex.CountrySectorVertexBuilder builder() {
        return new CountrySectorVertex.CountrySectorVertexBuilder();
    }

    /**
     * Creates a CountrySectorVertex from a country-sector ID string.
     * 
     * @param countrySectorId Format: "COUNTRY_SECTOR" (e.g., "USA_MANUFACTURING")
     * @return CountrySectorVertex instance
     */
    public static CountrySectorVertex fromCountrySectorId(String countrySectorId) {
        if (countrySectorId == null || !countrySectorId.contains("_")) {
            throw new IllegalArgumentException("Country-sector ID must be in format 'COUNTRY_SECTOR'");
        }
        
        String[] parts = countrySectorId.split("_", 2);
        String country = parts[0];
        String sector = parts[1];
        
        return CountrySectorVertex.builder()
                .id(countrySectorId)
                .pk(countrySectorId)
                .country(country)
                .sector(sector)
                .idStr(countrySectorId)
                .build();
    }

    public String getId() {
        return this.id;
    }

    public String getPk() {
        return this.pk;
    }

    public String getCountry() {
        return this.country;
    }

    public String getSector() {
        return this.sector;
    }

    public String getIdStr() {
        return this.idStr;
    }

    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof CountrySectorVertex)) return false;
        CountrySectorVertex other = (CountrySectorVertex) o;

        if (isNotEqual(id, other.id)) return false;
        if (isNotEqual(pk, other.pk)) return false;
        if (isNotEqual(country, other.country)) return false;
        if (isNotEqual(sector, other.sector)) return false;
        //noinspection RedundantIfStatement
        if (isNotEqual(idStr, other.idStr)) return false;

        return true;
    }

    private boolean isNotEqual(Object source, Object other) {
        if (source == null && other == null) return false;
        if (source == null) return true;
        return !source.equals(other);
    }

    public int hashCode() {
        int result = 59 + (id == null ? 43 : id.hashCode());
        result = result * 59 + (pk == null ? 43 : pk.hashCode());
        result = result * 59 + (country == null ? 43 : country.hashCode());
        result = result * 59 + (sector == null ? 43 : sector.hashCode());
        result = result * 59 + (idStr == null ? 43 : idStr.hashCode());
        return result;
    }

    public static class CountrySectorVertexBuilder {
        private String id;
        private String pk;
        private String country;
        private String sector;
        private String idStr;

        CountrySectorVertexBuilder() {
        }

        public CountrySectorVertex.CountrySectorVertexBuilder id(String id) {
            this.id = id;
            return this;
        }

        public CountrySectorVertex.CountrySectorVertexBuilder pk(String pk) {
            this.pk = pk;
            return this;
        }

        public CountrySectorVertex.CountrySectorVertexBuilder country(String country) {
            this.country = country;
            return this;
        }

        public CountrySectorVertex.CountrySectorVertexBuilder sector(String sector) {
            this.sector = sector;
            return this;
        }

        public CountrySectorVertex.CountrySectorVertexBuilder idStr(String idStr) {
            this.idStr = idStr;
            return this;
        }

        public CountrySectorVertex build() {
            return new CountrySectorVertex(this);
        }
    }
}
