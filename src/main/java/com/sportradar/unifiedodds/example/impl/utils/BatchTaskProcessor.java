package com.sportradar.unifiedodds.example.impl.utils;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchTaskProcessor {
    private static final Logger logger = LoggerFactory.getLogger(BatchTaskProcessor.class);

    private final String identifier;
    private final Executor executor;
    private final Map<String, List<Runnable>> batch;

    public BatchTaskProcessor(final String identifier,
                              final Executor executor,
                              final Map<String, List<Runnable>> batch) {
        Preconditions.checkNotNull(identifier);
        Preconditions.checkNotNull(executor);
        Preconditions.checkNotNull(batch);

        this.identifier = identifier;
        this.executor = executor;
        this.batch = batch;
    }

    public void processBatch(Runnable onBatchCompleted) {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        final AtomicInteger processedCount = new AtomicInteger(this.batch.size());
        final OnOrderedTasksCompletedCallback callback = () -> onProcessedCountChange(processedCount, stopwatch, onBatchCompleted);

        final int numOfRecords;
        final List<Runnable> runnables;
        {
            int numOfRecordsLocal = 0;
            runnables = new ArrayList<>(this.batch.size());
            for (final List<Runnable> group : this.batch.values()) {
                final ArrayList<Runnable> ordered = new ArrayList<>(group);
                numOfRecordsLocal += ordered.size();
                runnables.add(new OrderedTasks(ordered, callback, this.executor));
            }
            numOfRecords = numOfRecordsLocal;
        }

        logger.info("[{}] Processing '{}' records in '{}' queues", identifier, numOfRecords, runnables.size());
        try {
            for (final Runnable runnable : runnables) {
                this.executor.execute(runnable);
            }
        } catch (final Exception e) {
            logger.error("[{}] Failed to schedule records processing, exc:", identifier, e);
            tryStartNewBatch(stopwatch, onBatchCompleted);
        }
    }

    private synchronized void onProcessedCountChange(AtomicInteger processedCount,
                                                     Stopwatch stopwatch,
                                                     Runnable onBatchCompleted) {
        int i = processedCount.decrementAndGet();
        if (i > 0) {
            return;
        }

        tryStartNewBatch(stopwatch, onBatchCompleted);
    }

    private void tryStartNewBatch(Stopwatch stopwatch, Runnable onBatchCompleted) {
        logger.info("[{}] Batch processing completed, execution time: {}", identifier, stopwatch.elapsed());
        try {
            onBatchCompleted.run();
        } catch (Exception e) {
            logger.error("[{}] An exception occurred while restarting batch, exc: {}", identifier, e.getMessage(), e);
        }
    }

    @FunctionalInterface
    private interface OnOrderedTasksCompletedCallback {
        void onTasksCompleted();
    }

    private static final class OrderedTasks implements Runnable {
        private final ArrayList<Runnable> runnables;
        private final OnOrderedTasksCompletedCallback onOrderedTasksCompletedCallback;
        private final Executor executor;

        private int index;

        private OrderedTasks(final ArrayList<Runnable> runnables,
                             final OnOrderedTasksCompletedCallback onOrderedTasksCompletedCallback,
                             final Executor executor) {
            this.runnables = runnables;
            this.onOrderedTasksCompletedCallback = onOrderedTasksCompletedCallback;
            this.executor = executor;
            this.index = 0;
        }

        @Override
        public void run() {
            try {
                final Runnable runnable = this.runnables.get(this.index);
                if (runnable != null) {
                    runnable.run();
                }
            } catch (final Exception exc) {
                logger.error("Unexpected exc in run(): ", exc);
            } finally {
                this.index++;
                if (this.index < this.runnables.size()) {
                    this.enqueueNext();
                } else {
                    this.markAsDone();
                }
            }
        }

        private void enqueueNext() {
            try {
                this.executor.execute(this);
            } catch (final Exception exc) {
                logger.error("Unexpected exc in enqueueNext(): ", exc);
                this.markAsDone();
            }
        }

        private void markAsDone() {
            onOrderedTasksCompletedCallback.onTasksCompleted();
        }
    }
}