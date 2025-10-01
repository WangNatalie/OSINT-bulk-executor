// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.graph.bulk.sample;

import com.azure.graph.bulk.impl.model.GremlinEdgeVertexInfo;
import com.azure.graph.bulk.sample.model.DataGenerationException;
import com.azure.graph.bulk.sample.model.CountrySectorVertex;
import com.azure.graph.bulk.sample.model.SupplyEdge;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.azure.graph.bulk.sample.SeedGenerationValues.*;

public class GenerateDomainSamples {
    private GenerateDomainSamples() {
        throw new IllegalStateException("Utility class, should not be constructed");
    }

    public static List<CountrySectorVertex> getVertices(int volume) {
        try {
            SecureRandom random = SecureRandom.getInstanceStrong();

            return IntStream.range(1, volume + 1).mapToObj(
                            i -> generateCountrySector(random))
                    .collect(Collectors.toCollection(ArrayList::new));
        } catch (NoSuchAlgorithmException e) {
            throw new DataGenerationException(e);
        }
    }

    public static List<SupplyEdge> getEdges(List<CountrySectorVertex> vertices, int factor) {
        ArrayList<SupplyEdge> edges = new ArrayList<>();
        try {
            SecureRandom random = SecureRandom.getInstanceStrong();

            for (CountrySectorVertex vertex : vertices) {
                int volume = random.nextInt(factor) + 1;
                for (int i = 1; i <= volume; i++) {

                    edges.add(SupplyEdge.builder()
                            .sourceVertexInfo(GremlinEdgeVertexInfo.fromGremlinVertex(vertex))
                            .destinationVertexInfo(getRandomVertex(random, vertex.id, vertices))
                            .label("supplies")
                            .value(random.nextDouble() * 1000000.0)
                            .build());
                }
            }
        } catch (NoSuchAlgorithmException e) {
            throw new DataGenerationException(e);
        }
        return edges;
    }

    private static GremlinEdgeVertexInfo getRandomVertex(Random random, String sourceId, List<CountrySectorVertex> vertices) {
        GremlinEdgeVertexInfo vertex = null;
        while (vertex == null) {
            CountrySectorVertex potentialVertex = vertices.get(random.nextInt(vertices.size() - 1));
            if (!Objects.equals(potentialVertex.id, sourceId)) {
                vertex = GremlinEdgeVertexInfo.fromGremlinVertex(potentialVertex);
            }
        }
        return vertex;
    }

    private static CountrySectorVertex generateCountrySector(Random random) {
        String country = countries[random.nextInt(countries.length - 1)];
        String sector = sectors[random.nextInt(sectors.length - 1)];
        String id = country + "_" + sector;

        return CountrySectorVertex.builder()
                .id(id)
                .pk(id)
                .country(country)
                .sector(sector)
                .idStr(id)
                .build();
    }
}
