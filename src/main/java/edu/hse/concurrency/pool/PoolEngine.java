package edu.hse.concurrency.pool;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

final class PoolEngine {

    private final PoolConfig initialConfig;
    private final ConnectionFactory factory;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition idleChanged = lock.newCondition();
    private final Condition returned = lock.newCondition();
    private final Condition closingFinished = lock.newCondition();
    private final ArrayDeque<Idle> idle = new ArrayDeque<>();
    private final ArrayDeque<Waiter> waiting = new ArrayDeque<>();
    private final Set<Waiter> granted = new LinkedHashSet<>();
    private final Set<Lease> active = new HashSet<>();
    private final Thread evictionThread;
    private final long idleTimeoutNanos;

    private int maxSize;
    private int creating;
    private int borrowed;
    private int closing;
    private boolean stopped;

    PoolEngine(PoolConfig config, ConnectionFactory factory) {
        this.initialConfig = Objects.requireNonNull(config, "config");
        this.factory = Objects.requireNonNull(factory, "factory");
        maxSize = config.maxSize();
        idleTimeoutNanos = toNanos(config.idleTimeout());
        evictionThread = new Thread(this::evictIdle, "pool-idle-evictor");
        evictionThread.setDaemon(true);
        evictionThread.start();
    }

    PooledConnection borrow(Duration timeout)
            throws InterruptedException, PoolTimeoutException, ConnectionCreationException {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must not be negative");
        }

        long start = System.nanoTime();
        long timeoutNanos = toNanos(timeout);
        boolean create = false;
        RawConnection cancelled = null;

        lock.lock();
        try {
            checkRunning();

            if (waiting.isEmpty() && !idle.isEmpty()) {
                return takeIdle();
            }

            if (waiting.isEmpty() && total() < maxSize) {
                creating++;
                create = true;
            } else {
                if (timeoutNanos == 0) {
                    throw new PoolTimeoutException(timeout);
                }

                Waiter waiter = new Waiter(lock.newCondition());
                waiting.addLast(waiter);
                distribute();

                try {
                    while (waiter.grant == Grant.NONE && !stopped) {
                        long left = remaining(start, timeoutNanos);

                        if (left <= 0) {
                            cancelled = removeWaiter(waiter);
                            throw new PoolTimeoutException(timeout);
                        }

                        waiter.condition.awaitNanos(left);
                    }

                    if (stopped) {
                        cancelled = removeWaiter(waiter);
                        throw new IllegalStateException("Pool is shut down");
                    }

                    if (Thread.interrupted()) {
                        cancelled = removeWaiter(waiter);
                        throw new InterruptedException();
                    }

                    if (waiter.grant == Grant.IDLE) {
                        granted.remove(waiter);
                        waiter.grant = Grant.NONE;

                        Lease lease = new Lease(waiter.raw);
                        active.add(lease);
                        return lease;
                    }

                    granted.remove(waiter);
                    waiter.grant = Grant.NONE;
                    create = true;
                } catch (InterruptedException e) {
                    RawConnection alsoCancelled = removeWaiter(waiter);

                    if (alsoCancelled != null) {
                        cancelled = alsoCancelled;
                    }

                    throw e;
                }
            }
        } finally {
            lock.unlock();

            if (cancelled != null) {
                closeOne(cancelled);
            }
        }

        if (create) {
            return createConnection();
        }

        throw new IllegalStateException("Unexpected pool state");
    }

    void resize(int newMaxSize) {
        if (newMaxSize < 1
                || newMaxSize > PoolConfig.MAX_POOL_SIZE
                || newMaxSize < initialConfig.minIdle()) {
            throw new IllegalArgumentException("Invalid maxSize");
        }

        List<RawConnection> toClose = new ArrayList<>();

        lock.lock();
        try {
            checkRunning();
            maxSize = newMaxSize;

            while (total() > maxSize && !idle.isEmpty()) {
                toClose.add(idle.removeLast().raw);
                closing++;
            }

            List<Waiter> assignments = new ArrayList<>(granted);

            for (int i = assignments.size() - 1;
                 i >= 0 && total() > maxSize;
                 i--) {
                Waiter waiter = assignments.get(i);
                granted.remove(waiter);

                if (waiter.grant == Grant.IDLE) {
                    borrowed--;
                    closing++;
                    toClose.add(waiter.raw);
                    waiter.raw = null;
                } else {
                    creating--;
                }

                waiter.grant = Grant.NONE;
                waiting.addFirst(waiter);
            }

            distribute();
            idleChanged.signal();
        } finally {
            lock.unlock();
        }

        closeAll(toClose);
    }

    PoolConfig config() {
        lock.lock();
        try {
            return initialConfig.withMaxSize(maxSize);
        } finally {
            lock.unlock();
        }
    }

    PoolStats stats() {
        lock.lock();
        try {
            return new PoolStats(
                    total(),
                    idle.size(),
                    borrowed,
                    creating,
                    waiting.size()
            );
        } finally {
            lock.unlock();
        }
    }

    void shutdown(Duration grace) throws InterruptedException {
        Objects.requireNonNull(grace, "grace");

        if (grace.isNegative()) {
            throw new IllegalArgumentException("grace must not be negative");
        }

        long start = System.nanoTime();
        long graceNanos = toNanos(grace);
        List<RawConnection> toClose = new ArrayList<>();

        lock.lock();
        try {
            if (stopped) {
                return;
            }

            stopped = true;

            for (Waiter waiter : waiting) {
                waiter.condition.signal();
            }

            waiting.clear();

            for (Waiter waiter : granted) {
                if (waiter.grant == Grant.IDLE) {
                    borrowed--;
                    closing++;
                    toClose.add(waiter.raw);
                    waiter.raw = null;
                } else if (waiter.grant == Grant.CREATE) {
                    creating--;
                }

                waiter.grant = Grant.NONE;
                waiter.condition.signal();
            }

            granted.clear();

            while (!idle.isEmpty()) {
                closing++;
                toClose.add(idle.removeFirst().raw);
            }

            idleChanged.signalAll();
        } finally {
            lock.unlock();
        }

        closeAll(toClose);

        boolean interrupted = false;
        toClose = new ArrayList<>();

        lock.lock();
        try {
            long left = remaining(start, graceNanos);

            while (!active.isEmpty() && left > 0 && !interrupted) {
                try {
                    returned.awaitNanos(left);
                } catch (InterruptedException e) {
                    interrupted = true;
                }

                left = remaining(start, graceNanos);
            }

            for (Lease lease : active) {
                lease.valid = false;
                toClose.add(lease.raw);
                closing++;
                borrowed--;
            }

            active.clear();
        } finally {
            lock.unlock();
        }

        closeAll(toClose);

        boolean finished = false;

        while (!finished) {
            try {
                evictionThread.join();
                finished = true;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }

        lock.lock();
        try {
            while (closing != 0) {
                try {
                    closingFinished.await();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            lock.unlock();
        }

        if (interrupted) {
            throw new InterruptedException();
        }
    }

    private PooledConnection createConnection()
            throws ConnectionCreationException {
        RawConnection raw;

        try {
            raw = Objects.requireNonNull(
                    factory.create(),
                    "factory returned null"
            );
        } catch (Exception e) {
            lock.lock();
            try {
                creating--;
                distribute();

                if (stopped) {
                    throw new IllegalStateException("Pool is shut down", e);
                }
            } finally {
                lock.unlock();
            }

            throw new ConnectionCreationException(e);
        }

        boolean mustClose;

        lock.lock();
        try {
            creating--;
            mustClose = stopped;

            if (mustClose) {
                closing++;
            } else {
                borrowed++;

                Lease lease = new Lease(raw);
                active.add(lease);
                return lease;
            }
        } finally {
            lock.unlock();
        }

        closeOne(raw);

        throw new IllegalStateException(
                "Pool was shut down during connection creation"
        );
    }

    private Lease takeIdle() {
        RawConnection raw = idle.removeFirst().raw;
        borrowed++;

        Lease lease = new Lease(raw);
        active.add(lease);
        idleChanged.signal();

        return lease;
    }

    private int total() {
        return idle.size() + borrowed + creating;
    }

    private void checkRunning() {
        if (stopped) {
            throw new IllegalStateException("Pool is shut down");
        }
    }

    private void distribute() {
        if (stopped) {
            return;
        }

        while (!waiting.isEmpty()
                && (!idle.isEmpty() || total() < maxSize)) {
            Waiter waiter = waiting.removeFirst();

            if (waiter.grant != Grant.NONE) {
                continue;
            }

            if (!idle.isEmpty()) {
                waiter.grant = Grant.IDLE;
                waiter.raw = idle.removeFirst().raw;
                borrowed++;
                idleChanged.signal();
            } else {
                waiter.grant = Grant.CREATE;
                creating++;
            }

            granted.add(waiter);
            waiter.condition.signal();
        }
    }

    private RawConnection removeWaiter(Waiter waiter) {
        if (waiting.remove(waiter)) {
            return null;
        }

        if (!granted.remove(waiter)) {
            return null;
        }

        RawConnection toClose = null;

        if (waiter.grant == Grant.CREATE) {
            creating--;
        } else if (waiter.grant == Grant.IDLE) {
            boolean discard = stopped || total() > maxSize;
            borrowed--;

            if (discard) {
                closing++;
                toClose = waiter.raw;
            } else {
                idle.addLast(new Idle(
                        waiter.raw,
                        System.nanoTime()
                ));
                idleChanged.signal();
            }
        }

        waiter.raw = null;
        waiter.grant = Grant.NONE;
        distribute();

        return toClose;
    }

    private void evictIdle() {
        while (true) {
            RawConnection close = null;

            lock.lock();
            try {
                while (close == null && !stopped) {
                    if (idle.size() <= initialConfig.minIdle()) {
                        idleChanged.await();
                    } else {
                        Idle oldest = idle.peekFirst();

                        long left = idleTimeoutNanos
                                - (System.nanoTime() - oldest.since);

                        if (left > 0) {
                            idleChanged.awaitNanos(left);
                        } else {
                            close = idle.removeFirst().raw;
                            closing++;
                            distribute();
                        }
                    }
                }

                if (stopped) {
                    return;
                }
            } catch (InterruptedException e) {
                if (stopped) {
                    return;
                }
            } finally {
                lock.unlock();
            }

            if (close != null) {
                closeOne(close);
            }
        }
    }

    private void closeAll(List<RawConnection> connections) {
        for (RawConnection raw : connections) {
            closeOne(raw);
        }
    }

    private void closeOne(RawConnection raw) {
        try {
            raw.close();
        } catch (Exception ignored) {
        } finally {
            lock.lock();
            try {
                closing--;
                closingFinished.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    private static long toNanos(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private static long remaining(long started, long limit) {
        return limit - (System.nanoTime() - started);
    }

    private enum Grant {
        NONE,
        IDLE,
        CREATE
    }

    private static final class Waiter {
        private final Condition condition;
        private Grant grant = Grant.NONE;
        private RawConnection raw;

        private Waiter(Condition condition) {
            this.condition = condition;
        }
    }

    private static final class Idle {
        private final RawConnection raw;
        private final long since;

        private Idle(RawConnection raw, long since) {
            this.raw = raw;
            this.since = since;
        }
    }

    private final class Lease implements PooledConnection {
        private final RawConnection raw;
        private boolean valid = true;

        private Lease(RawConnection raw) {
            this.raw = raw;
        }

        @Override
        public RawConnection raw() {
            lock.lock();
            try {
                if (!valid) {
                    throw new IllegalStateException(
                            "Connection was returned or closed"
                    );
                }

                return raw;
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void close() {
            RawConnection close = null;

            lock.lock();
            try {
                if (!valid) {
                    return;
                }

                valid = false;
                active.remove(this);

                boolean discard = stopped || total() > maxSize;
                borrowed--;

                if (discard) {
                    closing++;
                    close = raw;
                    distribute();
                } else {
                    idle.addLast(new Idle(
                            raw,
                            System.nanoTime()
                    ));
                    idleChanged.signal();
                    distribute();
                }

                returned.signalAll();
            } finally {
                lock.unlock();
            }

            if (close != null) {
                closeOne(close);
            }
        }
    }
}
