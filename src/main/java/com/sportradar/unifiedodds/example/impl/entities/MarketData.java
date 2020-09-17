package com.sportradar.unifiedodds.example.impl.entities;

import java.util.Map;

/**
 * Created on 17. 09. 20
 *
 * @author e.roznik
 */
public class MarketData {
    private final int producerId;
    private final String eventId;
    private final int id;
    private final Map<String, String> specifiers;

    public MarketData(int producerId, String eventId, int id, Map<String, String> specifiers) {
        this.producerId = producerId;
        this.eventId = eventId;
        this.id = id;
        this.specifiers = specifiers;
    }

    public int getProducerId() {
        return producerId;
    }

    public String getEventId() {
        return eventId;
    }

    public int getId() {
        return id;
    }

    public Map<String, String> getSpecifiers() {
        return specifiers;
    }
}
