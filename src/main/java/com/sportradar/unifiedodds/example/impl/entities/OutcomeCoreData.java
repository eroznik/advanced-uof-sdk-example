package com.sportradar.unifiedodds.example.impl.entities;

/**
 * Created on 17. 09. 20
 *
 * @author e.roznik
 */
public class OutcomeCoreData {
    private final String id;
    private final Double probability;
    private final Double odds;
    private final boolean active;

    public OutcomeCoreData(String id, Double probability, Double odds, boolean active) {
        this.id = id;
        this.probability = probability;
        this.odds = odds;
        this.active = active;
    }

    public String getId() {
        return id;
    }

    public Double getProbability() {
        return probability;
    }

    public Double getOdds() {
        return odds;
    }

    public boolean isActive() {
        return active;
    }
}
