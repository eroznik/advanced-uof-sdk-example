package com.sportradar.unifiedodds.example.impl;

import com.google.common.base.Preconditions;
import com.sportradar.unifiedodds.example.impl.entities.MarketData;
import com.sportradar.utils.URN;

/**
 * Created on 17. 09. 20
 *
 * @author e.roznik
 */
public final class RedisKeysBuilder {
    private RedisKeysBuilder() {
        // no instance
    }

    public static String getHeartbeatKey(int producerId) {
        return String.format("producer:%s:heartbeat", producerId);
    }

    public static String getEventKey(URN eventId) {
        Preconditions.checkNotNull(eventId);
        return "event:" + eventId.toString();
    }

    public static String getMarketKey(MarketData marketData) {
        Preconditions.checkNotNull(marketData);
        return getMarketKey(FullMarketIdBuilder.composeFullMarketKey(marketData));
    }

    public static String getMarketKey(String fullMarketKey) {
        Preconditions.checkNotNull(fullMarketKey);
        return "market:" + fullMarketKey;
    }
}
