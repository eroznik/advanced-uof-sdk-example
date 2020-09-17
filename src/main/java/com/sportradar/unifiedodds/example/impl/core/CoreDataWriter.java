package com.sportradar.unifiedodds.example.impl.core;

import com.google.common.base.Preconditions;
import com.sportradar.unifiedodds.example.impl.JsonSerialization;
import com.sportradar.unifiedodds.example.impl.RedisKeysBuilder;
import com.sportradar.unifiedodds.example.impl.entities.MarketCoreData;
import com.sportradar.unifiedodds.example.impl.entities.OutcomeCoreData;
import com.sportradar.unifiedodds.sdk.entities.EventClock;
import com.sportradar.unifiedodds.sdk.entities.EventStatus;
import com.sportradar.unifiedodds.sdk.entities.status.CompetitionStatus;
import com.sportradar.unifiedodds.sdk.entities.status.MatchStatus;
import com.sportradar.unifiedodds.sdk.oddsentities.MarketWithOdds;
import com.sportradar.unifiedodds.sdk.oddsentities.OddsDisplayType;
import com.sportradar.unifiedodds.sdk.oddsentities.OutcomeOdds;
import com.sportradar.utils.URN;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created on 17. 09. 20
 *
 * @author e.roznik
 */
public class CoreDataWriter {
    private final JedisPool jedisPool;

    public CoreDataWriter(JedisPool jedisPool) {
        Preconditions.checkNotNull(jedisPool);

        this.jedisPool = jedisPool;
    }

    public void process(URN eventId, CompetitionStatus status) {
        Preconditions.checkNotNull(eventId);
        Preconditions.checkNotNull(status);

        // sample properties for demo purposes
        EventStatus eventStatus = status.getStatus();
        String eventTime = null;
        if (status instanceof MatchStatus) {
            EventClock eventClock = ((MatchStatus) status).getEventClock();
            if (eventClock != null) {
                eventTime = eventClock.getEventTime();
            }
        }

        Map<String, String> properties = new HashMap<>();
        if (eventStatus != null) {
            properties.put("eventStatus", eventStatus.toString());
        }
        if (eventTime != null) {
            properties.put("eventTime", eventTime);
        }

        if (properties.isEmpty()) {
            return;
        }

        try (Jedis client = jedisPool.getResource()) {
            client.hset(RedisKeysBuilder.getEventKey(eventId), properties);
        }
    }

    public void process(int producerId, URN eventId, List<MarketWithOdds> markets) {
        if (markets == null || markets.isEmpty()) {
            return;
        }

        List<MarketCoreData> mappedCoreData =
                markets.stream().map(v -> mapCoreData(producerId, eventId, v)).collect(Collectors.toList());

        if (mappedCoreData.isEmpty()) {
            return;
        }

        try (Jedis client  = jedisPool.getResource()) {

            long now = System.currentTimeMillis();
            for (MarketCoreData market : mappedCoreData) {
                Map<String, String> properties = new HashMap<>();

                properties.put("status", market.getMarketStatus().toString());
                properties.put("statusTs", now + "");

                for (OutcomeCoreData outcome : market.getOutcomes()) {
                    properties.put("os_" + outcome.getId(), JsonSerialization.serialize(outcome));
                }

                client.hset(RedisKeysBuilder.getMarketKey(market), properties);
            }
        }
    }

    private static MarketCoreData mapCoreData(int producerId, URN eventId, MarketWithOdds market) {
        Preconditions.checkNotNull(eventId);
        Preconditions.checkNotNull(market);

        return new MarketCoreData(
                producerId,
                eventId.toString(),
                market.getId(),
                market.getSpecifiers(),
                market.getStatus(),
                prepareOutcomes(market.getOutcomeOdds()));
    }

    private static List<OutcomeCoreData> prepareOutcomes(List<OutcomeOdds> outcomeOdds) {
        if (outcomeOdds == null || outcomeOdds.isEmpty()) {
            return Collections.emptyList();
        }
        return outcomeOdds.stream()
                .map(v -> new OutcomeCoreData(v.getId(), v.getProbability(), v.getOdds(OddsDisplayType.Decimal), v.isActive()))
                .collect(Collectors.toList());
    }
}
