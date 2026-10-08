import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class HW4 {

    enum TaskDistribution {
        UNIFORM,
        PERIODIC,
        PARETO
    }

    interface Shutdownable {
        void shutdown();
    }

    interface ShutdownableExecutor extends Shutdownable, Executor {
    }

    static class ThreadPerTaskExecutor implements ShutdownableExecutor {

        List<Thread> threads = new ArrayList<Thread>();

        @Override
        public void execute(Runnable command) {
            Thread thread = new Thread(command);
            threads.add(thread);
            thread.start();
        }

        @Override
        public void shutdown() {
            for (int i = 0; i < threads.size(); i++) {
                try {
                    threads.get(i).join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    static class RoundRobinExecutor implements ShutdownableExecutor {

        protected static final Runnable EXIT_TASK = new Runnable() {
            @Override
            public void run() {
            }
        };

        AtomicInteger counter = new AtomicInteger(0);

        List<Thread> threads;
        Vector<BlockingDeque<Runnable>> tasks;
        volatile boolean isShuttingDown = false;

        RoundRobinExecutor(int threadCount) {
            tasks = new Vector<BlockingDeque<Runnable>>();
            for (int i = 0; i < threadCount; i++) {
                tasks.add(new LinkedBlockingDeque<Runnable>());
            }

            threads = new ArrayList<Thread>();
            for (int i = 0; i < threadCount; i++) {
                threads.add(spawnThread(i));
            }
            for (int i = 0; i < threads.size(); i++) {
                threads.get(i).start();
            }
        }

        Thread spawnThread(int id) {
            Runnable body = new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        Runnable task;
                        try {
                            task = tasks.get(id).takeFirst();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        if (task == EXIT_TASK) {
                            return;
                        }
                        task.run();
                    }
                }
            };
            return new Thread(body);
        }

        @Override
        public void shutdown() {
            synchronized (this) {
                if (!isShuttingDown) {
                    isShuttingDown = true;
                    for (int i = 0; i < tasks.size(); i++) {
                        tasks.get(i).addLast(EXIT_TASK);
                    }
                }
            }

            for (int i = 0; i < threads.size(); i++) {
                try {
                    threads.get(i).join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public synchronized void execute(Runnable command) {
            if (isShuttingDown) {
                throw new RejectedExecutionException("Executor is shutting down");
            }
            tasks.get(counter.addAndGet(1) % threads.size()).addLast(command);
        }
    }

    static class WorkStealingExecutor extends RoundRobinExecutor {

        WorkStealingExecutor(int threadCount) {
            super(threadCount);
        }

        @Override
        Thread spawnThread(int id) {
            Runnable body = new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        Runnable task = tasks.get(id).pollFirst();
                        if (task == null) {
                            task = stealTask(id);
                        }

                        if (task == null) {
                            try {
                                task = tasks.get(id).pollFirst(1, TimeUnit.MILLISECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                            if (task == null) {
                                continue;
                            }
                        }

                        if (task == EXIT_TASK) {
                            task = stealTask(id);
                            if (task == null) {
                                return;
                            }
                            tasks.get(id).addLast(EXIT_TASK);
                        }

                        task.run();
                    }
                }
            };
            return new Thread(body);
        }

        private Runnable stealTask(int thiefId) {
            for (int offset = 1; offset < tasks.size(); offset++) {
                BlockingDeque<Runnable> queue = tasks.get((thiefId + offset) % tasks.size());
                Runnable task = queue.pollLast();

                if (task == EXIT_TASK) {
                    task = queue.pollLast();
                    queue.addLast(EXIT_TASK);
                }

                if (task != null) {
                    return task;
                }
            }
            return null;
        }
    }

    static class FixedThreadPoolExecutor implements ShutdownableExecutor {

        ExecutorService pool;

        FixedThreadPoolExecutor(int threadCount) {
            pool = Executors.newFixedThreadPool(threadCount);
        }

        @Override
        public void execute(Runnable command) {
            pool.execute(command);
        }

        @Override
        public void shutdown() {
            pool.shutdown();
            try {
                pool.awaitTermination(1, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    static class Pair {

        long first;
        long second;

        Pair(long first, long second) {
            this.first = first;
            this.second = second;
        }

        long getFirst() {
            return first;
        }

        long getSecond() {
            return second;
        }
    }

    static volatile long sink;

    static void blackHole(long difficulty) {
        long x = 0;
        for (long i = 0; i < difficulty; i++) {
            x += i ^ (x >>> 3);
        }
        sink = x;
    }

    static final int THREAD_NUMBER = 10;
    static final int TASK_NUMBER = 100_000;
    static final int TARGET_OPTIMAL_FULL_TIME = 10_000;

    static final int MEAN_TASK_TIME = (int) Math.round((TARGET_OPTIMAL_FULL_TIME + 0d) / TASK_NUMBER * THREAD_NUMBER);
    static final int LOWER_TASK_TIME_BOUND = 0;
    static final int HIGHER_TASK_TIME_BOUND = MEAN_TASK_TIME * 2 + 1;
    static final int ESTIMATED_OPTIMAL_TIME = MEAN_TASK_TIME * TASK_NUMBER / THREAD_NUMBER;
    static final long TARGET_TOTAL_TASK_TIME = (long) TARGET_OPTIMAL_FULL_TIME * THREAD_NUMBER;
    static final long RANDOM_SEED = 42;
    static final double PARETO_SHAPE = 1.5;
    static final int PARETO_RAW_MAX_TASK_TIME = MEAN_TASK_TIME * 100;

    static Runnable createTask(int duration) {
        return new Runnable() {
            @Override
            public void run() {
                blackHole(duration * 1_000_000L);
            }
        };
    }

    static int createTaskDuration(TaskDistribution distribution, int taskId, Random random) {
        switch (distribution) {
            case UNIFORM:
                return random.nextInt(LOWER_TASK_TIME_BOUND, HIGHER_TASK_TIME_BOUND);
            case PERIODIC:
                if (taskId % THREAD_NUMBER == 0) {
                    return MEAN_TASK_TIME * THREAD_NUMBER;
                }
                return 0;
            case PARETO:
                double scale = MEAN_TASK_TIME * (PARETO_SHAPE - 1) / PARETO_SHAPE;
                double duration = scale / Math.pow(1 - random.nextDouble(), 1 / PARETO_SHAPE);
                return (int) Math.min(Math.round(duration), PARETO_RAW_MAX_TASK_TIME);
        }
        return 0;
    }

    static int[] createTaskDurations(TaskDistribution distribution) {
        Random random = new Random(RANDOM_SEED);
        int[] durations = new int[TASK_NUMBER];

        long totalDuration = 0;
        for (int taskId = 0; taskId < TASK_NUMBER; taskId++) {
            durations[taskId] = createTaskDuration(distribution, taskId, random);
            totalDuration += durations[taskId];
        }

        double scale = (double) TARGET_TOTAL_TASK_TIME / totalDuration;
        double remainder = 0;
        long normalizedTotal = 0;
        for (int taskId = 0; taskId < TASK_NUMBER; taskId++) {
            double scaledDuration = durations[taskId] * scale + remainder;
            durations[taskId] = (int) scaledDuration;
            remainder = scaledDuration - durations[taskId];
            normalizedTotal += durations[taskId];
        }

        for (int taskId = 0; normalizedTotal < TARGET_TOTAL_TASK_TIME; taskId++) {
            durations[taskId % TASK_NUMBER]++;
            normalizedTotal++;
        }
        for (int taskId = 0; normalizedTotal > TARGET_TOTAL_TASK_TIME; taskId++) {
            int index = taskId % TASK_NUMBER;
            if (durations[index] > 0) {
                durations[index]--;
                normalizedTotal--;
            }
        }

        return durations;
    }

    static Vector<Runnable> createTasks(TaskDistribution distribution) {
        int[] durations = createTaskDurations(distribution);
        Vector<Runnable> tasks = new Vector<Runnable>();

        for (int i = 0; i < TASK_NUMBER; i++) {
            tasks.add(createTask(durations[i]));
        }
        return tasks;
    }

    public static Pair measureExecutor(ShutdownableExecutor executor, TaskDistribution distribution) {
        Vector<Runnable> tasks = createTasks(distribution);

        long start = System.nanoTime();
        for (int i = 0; i < tasks.size(); i++) {
            executor.execute(tasks.get(i));
        }
        long submissionEnd = System.nanoTime();

        executor.shutdown();
        long finish = System.nanoTime();

        return new Pair(submissionEnd - start, finish - start);
    }

    public static void main(String[] args) {
        System.out.println("Target optimal time: " + TARGET_OPTIMAL_FULL_TIME);
        System.out.println("Estimated optimal time: " + ESTIMATED_OPTIMAL_TIME);

        Pair threadPerTaskResult = measureExecutor(new ThreadPerTaskExecutor(), TaskDistribution.UNIFORM);

        System.out.println("Thread per task executor submission time: " + threadPerTaskResult.getFirst() / 1_000_000d);
        System.out.println("Thread per task execution time: " + threadPerTaskResult.getSecond() / 1_000_000d);

        for (TaskDistribution distribution : TaskDistribution.values()) {
            System.out.println("Task distribution: " + distribution);

            Pair roundRobinResult = measureExecutor(new RoundRobinExecutor(THREAD_NUMBER), distribution);

            System.out.println("Round Robin Executor submission time: " + roundRobinResult.getFirst() / 1_000_000d);
            System.out.println("Round Robin Executor execution time: " + roundRobinResult.getSecond() / 1_000_000d);

            Pair workStealingResult = measureExecutor(new WorkStealingExecutor(THREAD_NUMBER), distribution);

            System.out.println("Work Stealing Executor submission time: " + workStealingResult.getFirst() / 1_000_000d);
            System.out.println("Work Stealing Executor execution time: " + workStealingResult.getSecond() / 1_000_000d);

            Pair fixedPoolResult = measureExecutor(new FixedThreadPoolExecutor(THREAD_NUMBER), distribution);

            System.out.println("Fixed Thread Pool submission time: " + fixedPoolResult.getFirst() / 1_000_000d);
            System.out.println("Fixed Thread Pool execution time: " + fixedPoolResult.getSecond() / 1_000_000d);
        }
    }
}



/*
Target optimal time: 10000
Estimated optimal time: 10000
Thread per task executor submission time: 35016.389
Thread per task execution time: 38487.4316
Task distribution: UNIFORM
Round Robin Executor submission time: 97.6198
Round Robin Executor execution time: 11563.9104
Work Stealing Executor submission time: 22.4102
Work Stealing Executor execution time: 8944.992
Fixed Thread Pool submission time: 15.8484
Fixed Thread Pool execution time: 9043.1453
Task distribution: PERIODIC
Round Robin Executor submission time: 212.1989
Round Robin Executor execution time: 71119.8826
Work Stealing Executor submission time: 5.6226
Work Stealing Executor execution time: 8365.826599
Fixed Thread Pool submission time: 14.3177
Fixed Thread Pool execution time: 8226.0025
Task distribution: PARETO
Round Robin Executor submission time: 5.5551
Round Robin Executor execution time: 8882.833
Work Stealing Executor submission time: 5.597
Work Stealing Executor execution time: 8419.254099
Fixed Thread Pool submission time: 5.792801
Fixed Thread Pool execution time: 8448.868601
 */