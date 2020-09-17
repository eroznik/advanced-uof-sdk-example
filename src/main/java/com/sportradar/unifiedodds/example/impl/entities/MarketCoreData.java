package com.sportradar.unifiedodds.example.impl.entities;

import com.sportradar.unifiedodds.sdk.oddsentities.MarketStatus;

import java.util.List;
import java.util.Map;

/**
 * Created on 17. 09. 20
 *
 * @author e.roznik
 */
public class MarketCoreData extends MarketData {
    private final MarketStatus marketStatus;
    private final List<OutcomeCoreData> outcomes;

    public MarketCoreData(int producerId, String eventId, int id, Map<String, String> specifiers, MarketStatus marketStatus, List<OutcomeCoreData> outcomes) {
        super(producerId, eventId, id, specifiers);
        this.marketStatus = marketStatus;
        this.outcomes = outcomes;
    }

    public MarketStatus getMarketStatus() {
        return marketStatus;
    }

    public List<OutcomeCoreData> getOutcomes() {
        return outcomes;
    }
}
