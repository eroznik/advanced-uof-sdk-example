package com.sportradar.unifiedodds.example.impl;

import com.google.common.base.Preconditions;
import com.sportradar.unifiedodds.example.impl.core.CoreDataWriter;
import com.sportradar.unifiedodds.example.impl.meta.MetadataWriter;
import com.sportradar.unifiedodds.sdk.OddsFeedListener;
import com.sportradar.unifiedodds.sdk.OddsFeedSession;
import com.sportradar.unifiedodds.sdk.SDKGlobalEventsListener;
import com.sportradar.unifiedodds.sdk.entities.Competition;
import com.sportradar.unifiedodds.sdk.entities.SportEvent;
import com.sportradar.unifiedodds.sdk.oddsentities.BetCancel;
import com.sportradar.unifiedodds.sdk.oddsentities.BetSettlement;
import com.sportradar.unifiedodds.sdk.oddsentities.BetStop;
import com.sportradar.unifiedodds.sdk.oddsentities.FixtureChange;
import com.sportradar.unifiedodds.sdk.oddsentities.OddsChange;
import com.sportradar.unifiedodds.sdk.oddsentities.Producer;
import com.sportradar.unifiedodds.sdk.oddsentities.ProducerDown;
import com.sportradar.unifiedodds.sdk.oddsentities.ProducerStatus;
import com.sportradar.unifiedodds.sdk.oddsentities.ProducerUp;
import com.sportradar.unifiedodds.sdk.oddsentities.RollbackBetCancel;
import com.sportradar.unifiedodds.sdk.oddsentities.RollbackBetSettlement;
import com.sportradar.unifiedodds.sdk.oddsentities.UnparsableMessage;
import com.sportradar.utils.URN;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created on 17. 09. 20
 *
 * @author e.roznik
 */
public class FeedEventListener implements OddsFeedListener, SDKGlobalEventsListener {
    private static final Logger logger = LoggerFactory.getLogger(FeedEventListener.class);

    private final CoreDataWriter coreDataWriter;
    private final MetadataWriter metadataWriter;
    private final HeartbeatTracker heartbeatTracker;

    public FeedEventListener(CoreDataWriter coreDataWriter, MetadataWriter metadataWriter, HeartbeatTracker heartbeatTracker) {
        Preconditions.checkNotNull(coreDataWriter);
        Preconditions.checkNotNull(metadataWriter);
        Preconditions.checkNotNull(heartbeatTracker);

        this.coreDataWriter = coreDataWriter;
        this.metadataWriter = metadataWriter;
        this.heartbeatTracker = heartbeatTracker;
    }

    @Override
    public void onOddsChange(OddsFeedSession sender, OddsChange<SportEvent> oddsChanges) {
        int producerId = oddsChanges.getProducer().getId();
        URN eventId = oddsChanges.getEvent().getId();
        logger.info("Received odds change for Event[{}], producer[{}]", eventId, producerId);
        coreDataWriter.process(producerId, eventId, oddsChanges.getMarkets());
        if (oddsChanges.getEvent() instanceof Competition) {
            ((Competition) oddsChanges.getEvent())
                    .getStatusIfPresent()
                    .ifPresent(v -> coreDataWriter.process(eventId, v));
        }
        metadataWriter.process(oddsChanges.getEvent(), false);
        metadataWriter.process(producerId, eventId, oddsChanges.getMarkets());
    }

    @Override
    public void onFixtureChange(OddsFeedSession sender, FixtureChange<SportEvent> fixtureChange) {
        logger.info("Received fixture change for Event[{}], producer[{}]",
                fixtureChange.getEvent().getId(), fixtureChange.getProducer().getId());
        metadataWriter.process(fixtureChange.getEvent(), true);
    }

    @Override
    public void onBetStop(OddsFeedSession sender, BetStop<SportEvent> betStop) {
        logger.info("Received betstop for Event[{}], producer[{}]",
                betStop.getEvent().getId(), betStop.getProducer().getId());
        // TODO handling
    }

    @Override
    public void onBetSettlement(OddsFeedSession sender, BetSettlement<SportEvent> clearBets) {
        logger.info("Received bet settlement for Event[{}], producer[{}]",
                clearBets.getEvent().getId(), clearBets.getProducer().getId());
        // TODO handling
    }

    @Override
    public void onRollbackBetSettlement(OddsFeedSession sender, RollbackBetSettlement<SportEvent> rollbackBetSettlement) {
        logger.info("Received rollback betsettlement for Event[{}], producer[{}]",
                rollbackBetSettlement.getEvent().getId(), rollbackBetSettlement.getProducer().getId());
        // TODO handling
    }

    @Override
    public void onBetCancel(OddsFeedSession sender, BetCancel<SportEvent> betCancel) {
        logger.info("Received bet cancel for Event[{}], producer[{}]",
                betCancel.getEvent().getId(), betCancel.getProducer().getId());
        // TODO handling
    }

    @Override
    public void onRollbackBetCancel(OddsFeedSession sender, RollbackBetCancel<SportEvent> rbBetCancel) {
        logger.info("Received rollback betcancel for Event[{}], producer[{}]",
                rbBetCancel.getEvent().getId(), rbBetCancel.getProducer().getId());
        // TODO handling
    }

    @Override
    public void onUnparsableMessage(OddsFeedSession sender, UnparsableMessage unparsableMessage) {
        Producer possibleProducer = unparsableMessage.getProducer(); // the SDK will try to provide the origin of the message

        if (unparsableMessage.getEvent() != null) {
            logger.info("Problems detected on received message for event " + unparsableMessage.getEvent().getId());
            // TODO bet stop handling for the event
        } else {
            logger.info("Problems detected on received message"); // probably a system message deserialization failure
        }
    }

    @Override
    public void onConnectionDown() {
        logger.warn("onConnectionDown triggered!");
    }

    @Override
    public void onProducerStatusChange(ProducerStatus producerStatus) {
        logger.warn("Producer[{}] status changed to: '{}', isDelayed: {}, isDown: {}",
                producerStatus.getProducer().getId(), producerStatus.getProducerStatusReason(), producerStatus.isDelayed(), producerStatus.isDown());
        boolean isHealthy = !producerStatus.isDown() && !producerStatus.isDelayed();
        heartbeatTracker.onProducerStatusChange(producerStatus.getProducer().getId(), isHealthy);
    }

    @Override
    public void onProducerDown(ProducerDown producerDown) {
        // no-op, relying on onProducerStatusChange
    }

    @Override
    public void onProducerUp(ProducerUp producerUp) {
        // no-op relying on onProducerStatusChange
    }

    @Override
    public void onEventRecoveryCompleted(URN urn, long l) {
        // example doesnt do event recoveries
    }

    @Override
    public void onUnparseableMessage(OddsFeedSession sender, byte[] rawMessage, SportEvent event) {
        // no-op, deprecated
    }
}
