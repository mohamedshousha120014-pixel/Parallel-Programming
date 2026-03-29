package com.example.etl.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;

/**
 * CHUNK LISTENER — Runs before and after each chunk
 *
 * A chunk is a mini-batch of records (size defined by chunk-size property).
 * This listener logs which thread processes each chunk,
 * clearly showing multi-threading in action.
 *
 * Design Pattern: Observer Pattern
 * - Reacts to chunk lifecycle events
 */
public class ChunkProcessingListener implements ChunkListener {

    private static final Logger log = LoggerFactory.getLogger(ChunkProcessingListener.class);

    @Override
    public void beforeChunk(ChunkContext context) {
        log.debug("[{}] ▶ Starting new chunk...", Thread.currentThread().getName());
    }

    @Override
    public void afterChunk(ChunkContext context) {
        log.info("[{}] ✔ Chunk completed | Step: {}",
                Thread.currentThread().getName(),
                context.getStepContext().getStepName());
    }

    @Override
    public void afterChunkError(ChunkContext context) {
        log.error("[{}] ✖ Chunk FAILED in step: {}",
                Thread.currentThread().getName(),
                context.getStepContext().getStepName());
    }
}
