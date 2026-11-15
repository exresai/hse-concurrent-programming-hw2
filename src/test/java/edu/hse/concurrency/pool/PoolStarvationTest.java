package edu.hse.concurrency.pool;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PoolStarvationTest {

    private static final int THREADS = 4;

    @Test
    @Timeout(45)
    void nestedBorrowStarvesAtPoolSizeEqualToThreadCount() throws Exception {
        ConnectionPool pool = new ConnectionPoolImpl(
                new PoolConfig(0, THREADS, Duration.ofMinutes(5), true),
                new TestFactory()
        );

        CountDownLatch firstAcquired = new CountDownLatch(THREADS);
        CountDownLatch secondStarted = new CountDownLatch(THREADS);
        CountDownLatch finished = new CountDownLatch(THREADS);

        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread[] workers = new Thread[THREADS];

        try {
            for (int i = 0; i < THREADS; i++) {
                workers[i] = new Thread(() -> {
                    try (PooledConnection first =
                                 pool.borrow(Duration.ofSeconds(5))) {
                        firstAcquired.countDown();
                        firstAcquired.await();
                        secondStarted.countDown();

                        try (PooledConnection second =
                                     pool.borrow(Duration.ofMinutes(1))) {
                            second.raw();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Throwable e) {
                        failure.compareAndSet(null, e);
                    } finally {
                        finished.countDown();
                    }
                }, "pool-starvation-worker-" + i);

                workers[i].start();
            }

            assertTrue(secondStarted.await(5, TimeUnit.SECONDS));

            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(5);

            while (pool.stats().waiting() != THREADS
                    && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }

            assertEquals(
                    new PoolStats(THREADS, 0, THREADS, 0, THREADS),
                    pool.stats()
            );

            assertNull(failure.get());

            System.out.println(
                    "PID for jcmd: " + ProcessHandle.current().pid()
            );

            System.out.println(
                    "Waiting threads: pool-starvation-worker-0.."
                            + (THREADS - 1)
            );

            int seconds = Integer.parseInt(
                    System.getenv().getOrDefault("POOL_DUMP_SECONDS", "2")
            );

            assertFalse(finished.await(seconds, TimeUnit.SECONDS));
        } finally {
            for (Thread worker : workers) {
                if (worker != null) {
                    worker.interrupt();
                }
            }

            for (Thread worker : workers) {
                if (worker != null) {
                    worker.join(5_000);
                }
            }

            pool.shutdown(Duration.ZERO);
        }
    }

    @Test
    @Timeout(15)
    void oneExtraConnectionPreventsStarvation() throws Exception {
        int minimum = THREADS + 1;

        ConnectionPool pool = new ConnectionPoolImpl(
                new PoolConfig(0, minimum, Duration.ofMinutes(5), true),
                new TestFactory()
        );

        CountDownLatch firstAcquired = new CountDownLatch(THREADS);
        CountDownLatch finished = new CountDownLatch(THREADS);

        AtomicInteger successes = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Thread[] workers = new Thread[THREADS];

        try {
            for (int i = 0; i < THREADS; i++) {
                workers[i] = new Thread(() -> {
                    try (PooledConnection first =
                                 pool.borrow(Duration.ofSeconds(5))) {
                        firstAcquired.countDown();
                        firstAcquired.await();

                        try (PooledConnection second =
                                     pool.borrow(Duration.ofSeconds(5))) {
                            first.raw();
                            second.raw();
                            successes.incrementAndGet();
                        }
                    } catch (Throwable e) {
                        failure.compareAndSet(null, e);
                    } finally {
                        finished.countDown();
                    }
                }, "pool-progress-worker-" + i);

                workers[i].start();
            }

            assertTrue(finished.await(10, TimeUnit.SECONDS));
            assertNull(failure.get());
            assertEquals(THREADS, successes.get());
        } finally {
            for (Thread worker : workers) {
                if (worker != null) {
                    worker.interrupt();
                }
            }

            for (Thread worker : workers) {
                if (worker != null) {
                    worker.join(5_000);
                }
            }

            pool.shutdown(Duration.ZERO);
        }
    }

    private static final class TestFactory implements ConnectionFactory {

        @Override
        public RawConnection create() {
            return new RawConnection() {
                @Override
                public void close() {
                }
            };
        }
    }
}
