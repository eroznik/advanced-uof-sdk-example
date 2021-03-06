package com.sportradar.unifiedodds.example.impl;

import com.sportradar.unifiedodds.sdk.MessageInterest;
import com.sportradar.unifiedodds.sdk.OddsFeed;
import com.sportradar.unifiedodds.sdk.ProducerManager;
import com.sportradar.unifiedodds.sdk.cfg.OddsFeedConfiguration;
import com.sportradar.unifiedodds.sdk.exceptions.InitException;

import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Created on 17. 09. 20
 *
 * @author e.roznik
 */
public class MultithreadedSessionSetup {
    private final OddsFeed oddsFeed;
    private final FeedEventListener feedEventListener;

    public MultithreadedSessionSetup(FeedEventListener feedEventListener) {
        OddsFeedConfiguration configuration = OddsFeed.getOddsFeedConfigurationBuilder()
                .setAccessTokenFromSystemVar()
                .selectProduction()
                .setSdkNodeId(-371)
                .setDefaultLocale(Locale.ENGLISH)
                .setDesiredLocales(Arrays.asList(Locale.ENGLISH, Locale.ITALIAN))
                .build();

        this.feedEventListener = feedEventListener;
        this.oddsFeed = new OddsFeed(feedEventListener, configuration);
    }

    public void run() throws InitException {
        setProducersRecoveryTimestamp();

        oddsFeed.getSessionBuilder()
                .setMessageInterest(MessageInterest.PrematchMessagesOnly)
                .setListener(feedEventListener)
                .build();
        oddsFeed.getSessionBuilder()
                .setMessageInterest(MessageInterest.LiveMessagesOnly)
                .setListener(feedEventListener)
                .build();

        oddsFeed.open();
    }

    public void stop() throws Exception {
        oddsFeed.close();
    }

    private void setProducersRecoveryTimestamp() {
        // using the timestamp from 1 hour back, in real case scenarios you need to monitor the timestamp for recovery
        // with the producerManager.getProducer(producerId).getTimestampForRecovery(); method
        long recoveryFromTimestamp = System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);

        ProducerManager producerManager = oddsFeed.getProducerManager();

        producerManager.getActiveProducers().values().forEach(p -> producerManager.setProducerRecoveryFromTimestamp(p.getId(), recoveryFromTimestamp));
    }
}
