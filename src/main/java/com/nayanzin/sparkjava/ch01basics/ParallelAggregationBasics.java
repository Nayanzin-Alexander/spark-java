package com.nayanzin.sparkjava.ch01basics;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static java.lang.Integer.min;
import static java.lang.Math.abs;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class ParallelAggregationBasics {

    private static final Random RAND = new Random(47);
    private static long timer;

    private static List<Integer> generateWorkload(@SuppressWarnings("SameParameterValue") int listSize) {
        startTimer();
        List<Integer> result = IntStream.range(0, listSize)
                .map(i -> RAND.nextInt(50))
                .boxed()
                .collect(toList());
        stopTimerAndPrintResult("generateWorkload");
        return result;
    }

    @SuppressWarnings("squid:S106")
    private static void stopTimerAndPrintResult(String generateWorkload) {
        long elapsedTime = abs(System.nanoTime() - timer);
        System.out.println(generateWorkload + " " + elapsedTime / 1000_000 + " millis");
    }

    private static void startTimer() {
        timer = System.nanoTime();
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        long[] results = new long[5];
        List<Integer> workload = generateWorkload(10_000_000);

        results[0] = runSumViaLoopUnboxed(workload);
        results[1] = runSumViaLoopUnboxedInParallel(workload, Runtime.getRuntime().availableProcessors() + 1);
        results[2] = runSumViaLoopBoxed(workload);
        results[3] = runSumViaStream(workload);
        results[4] = runSumViaStreamInParallel(workload);

        if (Arrays.stream(results).distinct().count() != 1) {
            throw new AssertionError(format("Results must be the same: %s", Arrays.toString(results)));
        }
    }

    private static long runSumViaLoopUnboxedInParallel(List<Integer> workload, int nThreads) throws ExecutionException, InterruptedException {
        startTimer();
        int offset = workload.size() / nThreads + workload.size() % nThreads; // analog of ceiling(size / nThreads)
        ExecutorService exec = Executors.newFixedThreadPool(nThreads);
        List<Future<Long>> results = IntStream.range(0, nThreads).boxed()
                .map(i -> exec.submit(
                        () -> loopSublistUnboxed(workload, i * offset, min(workload.size(), (i + 1) * offset))))
                .collect(toList());

        long accumulator = 0;
        for (Future<Long> future : results) {
            accumulator += future.get();
        }
        stopTimerAndPrintResult("loopUnboxedInParallel");
        exec.shutdown();
        return accumulator;
    }

    private static long runSumViaStreamInParallel(List<Integer> workload) {
        startTimer();
        long result = workload.stream()
                .parallel()
                .map(Long::valueOf)
                .reduce(Long::sum)
                .orElseThrow(RuntimeException::new);
        stopTimerAndPrintResult("sumParallelStream");
        return result;
    }

    private static long runSumViaLoopUnboxed(List<Integer> collection) {
        startTimer();
        long accumulator = 0;
        for (int value : collection) {
            accumulator += value;
        }
        stopTimerAndPrintResult("loopUnboxed");
        return accumulator;
    }

    private static long loopSublistUnboxed(List<Integer> collection, int startIndex, int endIndex) {
        long accumulator = 0;
        for (int i = startIndex; i < endIndex; i++) {
            accumulator += collection.get(i);
        }
        return accumulator;
    }

    private static long runSumViaLoopBoxed(Collection<Integer> collection) {
        startTimer();
        Long accumulator = 0L;
        for (Integer integer : collection) {
            accumulator += integer;
        }
        stopTimerAndPrintResult("loopBoxed");
        return accumulator;
    }

    private static long runSumViaStream(Collection<Integer> collection) {
        startTimer();
        long result = collection.stream()
                .map(Long::valueOf)
                .reduce(Long::sum)
                .orElseThrow(RuntimeException::new);
        stopTimerAndPrintResult("sumStream");
        return result;
    }
}
