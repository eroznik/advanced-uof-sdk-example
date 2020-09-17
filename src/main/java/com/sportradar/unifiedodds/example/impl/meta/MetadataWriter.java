package com.sportradar.unifiedodds.example.impl.meta;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.sportradar.unifiedodds.example.impl.FullMarketIdBuilder;
import com.sportradar.unifiedodds.example.impl.JsonSerialization;
import com.sportradar.unifiedodds.example.impl.RedisKeysBuilder;
import com.sportradar.unifiedodds.example.impl.entities.MarketData;
import com.sportradar.unifiedodds.example.impl.entities.OutcomeMetadata;
import com.sportradar.unifiedodds.example.impl.utils.BatchTaskProcessor;
import com.sportradar.unifiedodds.sdk.entities.SportEvent;
import com.sportradar.unifiedodds.sdk.oddsentities.Market;
import com.sportradar.unifiedodds.sdk.oddsentities.MarketWithOdds;
import com.sportradar.unifiedodds.sdk.oddsentities.OutcomeOdds;
import com.sportradar.utils.URN;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created on 17. 09. 20
 *
 * @author e.roznik
 */
public class MetadataWriter {
    private static final Logger logger = LoggerFactory.getLogger(MetadataWriter.class);

    private static final int BATCH_SIZE = 100;
    private static final int MAX_CONCURRENCY = 10;

    private final JedisPool jedisPool;
    private final ScheduledExecutorService metadataExecutor;

    private final Cache<String, Integer> eventMetaTracker;
    private final Cache<String, Integer> marketMetaTracker;

    private final BlockingQueue<RunnableTask> loadQueue;

    public MetadataWriter(JedisPool jedisPool, ScheduledExecutorService metadataExecutor) {
        Preconditions.checkNotNull(jedisPool);
        Preconditions.checkNotNull(metadataExecutor);

        this.jedisPool = jedisPool;
        this.metadataExecutor = metadataExecutor;

        this.eventMetaTracker =
                CacheBuilder.newBuilder()
                        .expireAfterWrite(1, TimeUnit.HOURS)
                        .build();
        this.marketMetaTracker =
                CacheBuilder.newBuilder()
                        .expireAfterWrite(1, TimeUnit.HOURS)
                        .build();

        this.loadQueue = new LinkedBlockingQueue<>();
    }

    public void start() {
        processMetaBatch();
    }

    public void process(SportEvent event, boolean forceReload) {
        Preconditions.checkNotNull(event);

        String eventId = event.getId().toString();
        if (eventMetaTracker.getIfPresent(eventId) == null || forceReload) {
            eventMetaTracker.put(eventId, 1);
            loadQueue.add(new RunnableTask(eventId, () -> writeEventMetadata(event)));
        }
    }

    public void process(int producerId, URN eventId, List<MarketWithOdds> markets) {
        if (markets == null || markets.isEmpty()) {
            return;
        }

        markets.forEach(m -> {
            MarketData marketData = new MarketData(producerId, eventId.toString(), m.getId(), m.getSpecifiers());
            String fullMarketId = FullMarketIdBuilder.composeFullMarketKey(marketData);
            if (marketMetaTracker.getIfPresent(fullMarketId) == null) {
                marketMetaTracker.put(fullMarketId, 1);
                loadQueue.add(new RunnableTask(fullMarketId, () -> writeMarketMetadata(fullMarketId, m)));
            }
        });
    }

    private void processMetaBatch() {
        AtomicInteger processedCount = new AtomicInteger(0);
        Map<String, List<Runnable>> metaTasks = new HashMap<>();
        for (int i = 0; i < BATCH_SIZE; i++) {
            RunnableTask metaLoadRequest = loadQueue.poll();
            if (metaLoadRequest == null) {
                break;
            }
            processedCount.incrementAndGet();

            metaTasks
                    .computeIfAbsent(metaLoadRequest.identifier, k -> new LinkedList<>())
                    .add(() -> {
                        try {
                            metaLoadRequest.runnable.run();
                        } catch (Exception e) {
                            logger.error("Failed to process meta[" + metaLoadRequest.identifier + "] request: " + e.getMessage(), e);
                        }
                    });

            if (metaTasks.size() >= MAX_CONCURRENCY) {
                // we must cap max concurrency so we do not clutter the executor
                break;
            }
        }

        if (processedCount.get() == 0) {
            scheduleAfterEmptyQueue();
            return;
        }

        BatchTaskProcessor bp = new BatchTaskProcessor("meta-batch-processor", metadataExecutor, metaTasks);
        bp.processBatch(this::scheduleAfterNotEmptyQueue);
    }

    private void scheduleAfterEmptyQueue() {
        metadataExecutor.schedule(this::processMetaBatch, 500, TimeUnit.MILLISECONDS);
    }

    private void scheduleAfterNotEmptyQueue() {
        metadataExecutor.submit(this::processMetaBatch);
    }

    private void writeMarketMetadata(String fullMarketId, MarketWithOdds market) {
        Preconditions.checkNotNull(market);

        Map<String, String> properties = new HashMap<>();
        String name = market.getName(Locale.ENGLISH);
        if (name != null) {
            properties.put("EN_name", name);
        }

        String italianName = market.getName(Locale.ITALIAN);
        if (italianName != null) {
            properties.put("IT_name", italianName);
        }

        if (market.getOutcomeOdds() != null && !market.getOutcomeOdds().isEmpty()) {
            processOutcomesForLocale("EN", Locale.ENGLISH, properties, market.getOutcomeOdds());
            processOutcomesForLocale("IT", Locale.ITALIAN, properties, market.getOutcomeOdds());
        }

        if (properties.isEmpty()) {
            logger.warn("MarketMetadata write skipped, no properties to write for: {}", fullMarketId);
            return;
        }

        properties.put("meTs", System.currentTimeMillis() + "");

        try (Jedis client = jedisPool.getResource()) {
            Pipeline pipelined = client.pipelined();

            pipelined.hmset(RedisKeysBuilder.getMarketKey(fullMarketId), properties);

            pipelined.sync();
        }
    }

    private static void processOutcomesForLocale(String prefixLocale, Locale locale, Map<String, String> properties, List<OutcomeOdds> outcomeOdds) {
        Preconditions.checkNotNull(prefixLocale);
        Preconditions.checkNotNull(locale);
        Preconditions.checkNotNull(properties);
        Preconditions.checkNotNull(outcomeOdds);

        for (OutcomeOdds outcome : outcomeOdds) {
            String name = outcome.getName(locale);
            if (name == null) {
                continue;
            }
            OutcomeMetadata data = new OutcomeMetadata(name);
            properties.put("om_" + prefixLocale + "_" + outcome.getId(), JsonSerialization.serialize(data));
        }
    }

    private void writeEventMetadata(SportEvent event) {
        Preconditions.checkNotNull(event);

        Map<String, String> properties = new HashMap<>();
        String name = event.getName(Locale.ENGLISH);
        if (name != null) {
            properties.put("EN_eventName", name);
        }

        String italianName = event.getName(Locale.ITALIAN);
        if (italianName != null) {
            properties.put("IT_eventName", italianName);
        }

        if (event.getScheduledTime() != null) {
            properties.put("startTime", event.getScheduledTime().toInstant().toEpochMilli() + "");
        }

        if (properties.isEmpty()) {
            logger.warn("EventMetadata write skipped, no properties to write for: {}", event.getId());
            return;
        }

        properties.put("meTs", System.currentTimeMillis() + "");

        try (Jedis client = jedisPool.getResource()) {
            Pipeline pipelined = client.pipelined();
            pipelined.hmset(RedisKeysBuilder.getEventKey(event.getId()), properties);
            pipelined.sync();
        }
    }

    private static class RunnableTask {
        private final String identifier;
        private final Runnable runnable;

        private RunnableTask(String identifier, Runnable runnable) {
            this.identifier = identifier;
            this.runnable = runnable;
        }
    }
}
