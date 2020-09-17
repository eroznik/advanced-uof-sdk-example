package com.sportradar.unifiedodds.example;

import com.sportradar.unifiedodds.example.impl.FeedEventListener;
import com.sportradar.unifiedodds.example.impl.HeartbeatTracker;
import com.sportradar.unifiedodds.example.impl.MultithreadedSessionSetup;
import com.sportradar.unifiedodds.example.impl.core.CoreDataWriter;
import com.sportradar.unifiedodds.example.impl.meta.MetadataWriter;

import java.util.concurrent.TimeUnit;

public class AdvancedOddsFeedExampleMain {

    public static void main(String[] args) throws Exception {
        CoreDataWriter coreDataWriter = new CoreDataWriter(); // important betting information processing
        MetadataWriter metadataWriter = new MetadataWriter(); // event/market metadata async processing
        HeartbeatTracker heartbeatTracker = new HeartbeatTracker(); // producer liveness tracking

        FeedEventListener feedEventListener = new FeedEventListener(coreDataWriter, metadataWriter, heartbeatTracker);

        MultithreadedSessionSetup multithreadedSessionSetup = new MultithreadedSessionSetup(feedEventListener);
        multithreadedSessionSetup.run();

        // sleep 30min for demo purposes
        TimeUnit.MINUTES.sleep(30);

        multithreadedSessionSetup.stop();
    }
}
