package com.sportradar.unifiedodds.example.impl;

import com.google.common.base.Preconditions;
import com.sportradar.unifiedodds.example.impl.entities.MarketData;

import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Created on 17. 09. 20
 *
 * @author e.roznik
 */
public final class FullMarketIdBuilder {
    private FullMarketIdBuilder() {
        // no instance
    }

    public static String composeFullMarketKey(MarketData marketData) {
        Preconditions.checkNotNull(marketData);

        return String.format("uof:%s/%s/%s%s", marketData.getProducerId(), marketData.getEventId(), marketData.getId(), composeSpecifiersTail(marketData.getSpecifiers()));
    }

    private static String composeSpecifiersTail(Map<String, String> specifiers) {
        if (specifiers == null || specifiers.isEmpty()) {
            return "";
        }
        return "/" + new TreeSet<>(specifiers.keySet()).stream()
                .map(v -> v + "=" + specifiers.get(v))
                .collect(Collectors.joining("|"));
    }
}
