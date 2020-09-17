package com.sportradar.unifiedodds.example;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sportradar.unifiedodds.example.impl.FeedEventListener;
import com.sportradar.unifiedodds.example.impl.HeartbeatTracker;
import com.sportradar.unifiedodds.example.impl.MultithreadedSessionSetup;
import com.sportradar.unifiedodds.example.impl.core.CoreDataWriter;
import com.sportradar.unifiedodds.example.impl.meta.MetadataWriter;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AdvancedOddsFeedExampleMain {

    public static void main(String[] args) throws Exception {
        // resources setup
        JedisPool jedisPool = getJedisPool();
        ScheduledExecutorService heartbeatTrackingExecutor =
                Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("heartbeat-t-%d").build());
        ScheduledExecutorService metadataExecutor =
                Executors.newScheduledThreadPool(16, new ThreadFactoryBuilder().setNameFormat("metadata-t-%d").build());

        // data processors
        CoreDataWriter coreDataWriter = new CoreDataWriter(jedisPool); // important betting information processing
        MetadataWriter metadataWriter = new MetadataWriter(jedisPool, metadataExecutor); // event/market metadata async processing
        HeartbeatTracker heartbeatTracker = new HeartbeatTracker(jedisPool, heartbeatTrackingExecutor); // producer liveness tracking

        heartbeatTracker.start();
        metadataWriter.start();

        // UOF consumer setup
        FeedEventListener feedEventListener = new FeedEventListener(coreDataWriter, metadataWriter, heartbeatTracker);
        MultithreadedSessionSetup multithreadedSessionSetup = new MultithreadedSessionSetup(feedEventListener);
        multithreadedSessionSetup.run();

        // sleep 30min for demo purposes
        TimeUnit.MINUTES.sleep(30);

        // resources cleanup
        multithreadedSessionSetup.stop();
        jedisPool.destroy();
        heartbeatTrackingExecutor.shutdownNow();
        metadataExecutor.shutdownNow();
    }

    private static JedisPool getJedisPool() throws URISyntaxException {
        int timeout = 500;
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(10);
        config.setMaxIdle(10);
        config.setMinIdle(10);
        config.setMaxWaitMillis(timeout);
        config.setBlockWhenExhausted(true);
        config.setTestOnBorrow(false);
        config.setTestOnCreate(false);
        config.setTestOnReturn(false);
        URI url = new URI("redis://localhost:6379");
        return new JedisPool(config, url, timeout, timeout);
    }
}
