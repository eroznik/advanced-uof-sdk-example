package com.sportradar.unifiedodds.example.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created on 17. 09. 20
 *
 * @author e.roznik
 */
public class HeartbeatTracker {
    private static final Logger logger = LoggerFactory.getLogger(HeartbeatTracker.class);

    private final JedisPool jedisPool;
    private final ScheduledExecutorService heartbeatTrackingExecutor;
    private final Map<Integer, AtomicBoolean> producerStatuses;

    public HeartbeatTracker(JedisPool jedisPool, ScheduledExecutorService heartbeatTrackingExecutor) {
        Preconditions.checkNotNull(jedisPool);
        Preconditions.checkNotNull(heartbeatTrackingExecutor);

        this.jedisPool = jedisPool;
        this.heartbeatTrackingExecutor = heartbeatTrackingExecutor;
        this.producerStatuses = Maps.newConcurrentMap();
    }

    public void start() {
        heartbeatTrackingExecutor.scheduleWithFixedDelay(this::writeHeartbeats, 10, 10, TimeUnit.SECONDS);
    }

    private void writeHeartbeats() {
        logger.info("Updating producer heartbeats");
        try (Jedis client = jedisPool.getResource()) {
            long currentTimestamp = System.currentTimeMillis();
            producerStatuses.forEach((pid, status) -> {
                if (status.get()) {
                    updateProducerHeartbeat(client, pid, currentTimestamp);
                } else {
                    logger.warn("Producer[{}] unhealthy, skipping producer heartbeat update", pid);
                }
            });
        }
    }

    private void updateProducerHeartbeat(Jedis client, Integer pid, long currentTimestamp) {
        logger.info("Producer[{}] is healthy, updating heartbeat value", pid);
        client.set(RedisKeysBuilder.getHeartbeatKey(pid), currentTimestamp + "");
    }

    public void onProducerStatusChange(int producerId, boolean isHavingIssues) {
        logger.info("Setting producer[{}] status to '{}'", producerId, !isHavingIssues);
        producerStatuses.computeIfAbsent(producerId, (x) -> new AtomicBoolean(false)).set(!isHavingIssues);
    }
}
