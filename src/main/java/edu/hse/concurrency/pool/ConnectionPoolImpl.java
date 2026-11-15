package edu.hse.concurrency.pool;

import java.time.Duration;

public class ConnectionPoolImpl implements ConnectionPool {

    private final PoolEngine engine;

    public ConnectionPoolImpl(PoolConfig config, ConnectionFactory factory) {
        engine = new PoolEngine(config, factory);
    }

    @Override
    public PooledConnection borrow(Duration timeout)
            throws InterruptedException, PoolTimeoutException, ConnectionCreationException {
        return engine.borrow(timeout);
    }

    @Override
    public void resize(int maxSize) {
        engine.resize(maxSize);
    }

    @Override
    public PoolConfig config() {
        return engine.config();
    }

    @Override
    public PoolStats stats() {
        return engine.stats();
    }

    @Override
    public void shutdown(Duration grace) throws InterruptedException {
        engine.shutdown(grace);
    }
}
