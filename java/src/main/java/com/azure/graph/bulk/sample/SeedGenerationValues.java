// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.graph.bulk.sample;

public class SeedGenerationValues {
    private SeedGenerationValues() {
        throw new IllegalStateException("Utility class, should not be constructed");
    }

    protected static final String[] countries = new String[]{
            "AFG", "USA", "ALB", "BHS", "BRA", "CHN", "CZE", "EGY", "GUM", "GIN", "HND", "HUN", "MDG", "MLI"
    };

    protected static final String[] sectors = new String[]{
            "MANUFACTURING", "AGRICULTURE", "MINING", "CONSTRUCTION", "TRANSPORTATION", "FINANCE", 
            "HEALTHCARE", "EDUCATION", "RETAIL", "TECHNOLOGY", "ENERGY", "TOURISM", "REAL_ESTATE",
            "ENTERTAINMENT", "FOOD_SERVICES", "TELECOMMUNICATIONS", "AUTOMOTIVE", "AEROSPACE",
            "CHEMICALS", "TEXTILES", "PHARMACEUTICALS", "UTILITIES", "GOVERNMENT", "DEFENSE"
    };

}
