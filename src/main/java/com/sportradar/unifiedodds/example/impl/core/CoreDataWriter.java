package com.sportradar.unifiedodds.example.impl.core;

import com.sportradar.unifiedodds.sdk.entities.status.CompetitionStatus;
import com.sportradar.unifiedodds.sdk.oddsentities.MarketWithOdds;
import com.sportradar.utils.URN;

import java.util.List;

/**
 * Created on 17. 09. 20
 *
 * @author e.roznik
 */
public class CoreDataWriter {
    public void process(int producerId, URN eventId, List<MarketWithOdds> markets) {

    }

    public void process(URN eventId, CompetitionStatus status) {

    }
}
