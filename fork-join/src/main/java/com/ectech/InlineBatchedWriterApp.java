package com.ectech;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * mvn clean package exec:exec
 * Discussion: This impl uses String model and recurses on each order-of-mag digit and writes to a Queue.
 * Then a single root action writes the Queue to disk. Works find if queue capacity is large.
 * If the queue capacity is small, many Actions will be blocked.
 *
 * Example using numbers: https://www.codejava.net/java-core/concurrency/understanding-java-fork-join-framework-with-examples
 *
 * Enhancement Options:
 * 1) Put Queue writer into its own thread or thread pool.
 * 2) Get rid of the queue and use the ForkJoin Dequeue, using multiple writer Actions.
 * It may be possible to use multiple tasks to write if there is an Atomic Lock.
 */
public class InlineBatchedWriterApp
{
    ForkJoinPool commonPool = ForkJoinPool.commonPool();
    public static void main( String[] args ) throws IOException {
        System.out.println("Starting InlineBatchedWriterApp");
        InlineBatchedWriterApp app = new InlineBatchedWriterApp();
        LocalDateTime dt = LocalDateTime.now();
        app.startCompute(7);
        Duration dur = Duration.between(dt, LocalDateTime.now());
        if(dur.toMillis() < 20*1000) {
            System.out.println(String.format("Completed in %d milliseconds", dur.toMillis()));
        } else if(dur.toMillis() < 120*1000) {
            System.out.println(String.format("Completed in %d:%02d:%02d, or %d milliseconds", dur.toHoursPart(), dur.toMinutesPart(), dur.toSecondsPart(), dur.toMillis()));
        } else {
            System.out.println(String.format("Completed in %d:%02d:%02d", dur.toHoursPart(), dur.toMinutesPart(), dur.toSecondsPart()));
        }
        // depth of 6 creates almost 6Mb
        // 7 creates 9 999 999 ~10M ~70Mb
        // 8 creates 100M=800Mb
        // 9 creates 1000M=9Gb
        // 10 creates 10,000M= 100Gb

        // each worker creates 10 entries, so 10M entries is created by 1M workers.
    }

    //   DagRoot
    //      writer
    //           Queue -> file
    //      writerNotifier
    //          workers -> Queue
    //            null ->
    protected void startCompute(int depth) throws IOException {
        String fileName = "/tmp/concurrent-combos-" + depth + ".txt";
        File combosFile = Paths.get(fileName).toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(combosFile))) {
            RecursiveTask rt = new CharRecursionTask(depth, bw);


            commonPool.invoke(rt);
        } catch (IOException e) {
            System.out.println(e.toString());
            throw new RuntimeException(e);
        }


        // System.out.println(String.format("Generated %d combos. remaining %d, written to: %s", generatedCombos.size(), filteredCombos.size(), fileName));
    }


    class CharRecursionTask extends RecursiveTask {
        protected String parentData;
        protected int nextDigit;
        protected int requiredDepth;
        protected int currentLevel;
        protected BufferedWriter fileWriter;
        public CharRecursionTask(int requiredDepth, BufferedWriter fileWriter) {
            this.requiredDepth = requiredDepth;
            this.currentLevel = 0;
            this.fileWriter = fileWriter;
        }
        public CharRecursionTask(String parentData, int newDigit, int requiredDepth, int currentLevel, BufferedWriter fileWriter) {
            this.parentData = parentData;
            this.nextDigit = newDigit;
            this.requiredDepth = requiredDepth;
            this.currentLevel = currentLevel;
            this.fileWriter = fileWriter;
        }
        // in the future, we may want to wrap the leaf nodes with an output Action.
        protected Stream<CharRecursionTask> generateNextLevel(String currPhoneValue) {
            return Stream.iterate(0, n -> n + 1)
                .limit(10).map(i -> new CharRecursionTask(currPhoneValue, i, requiredDepth, this.currentLevel+1, this.fileWriter));

        }
        @Override
        protected String compute() {
            String currValue = this.parentData == null ? "" : this.parentData + String.valueOf(this.nextDigit);
            if (this.currentLevel < this.requiredDepth) {
                List<CharRecursionTask> nextLevel = generateNextLevel(currValue).collect(Collectors.toList());
                Collection<String> generatedPhones = ForkJoinTask.invokeAll(nextLevel).stream().map(CharRecursionTask::join).map(o -> (String)o).collect(Collectors.toList());
                //Collection<String> generatedPhones = ForkJoinTask.invokeAll(nextLevel).stream().map(CharRecursionTask::join).flatMap(o -> Stream.of((String)o)).collect(Collectors.toList());
                if(this.currentLevel == this.requiredDepth -1) {
                    try {
                        for(String currPhone : generatedPhones) {
                            synchronized (fileWriter) {
                                fileWriter.write(currPhone + '\n');
                            }
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

            }
            return currValue;
        }
    }
    /**
     * Note that as structured, this class cannot be generalized such that the DAG generation is independent of the compute.
     * To separate DAG generation from compute, state must be encapsulated.
     */
    class PhoneGeneratorTask extends RecursiveTask<String> {
        protected String parentPhone;
        protected int nextDigit;

        public PhoneGeneratorTask(String phone, int newDigit) {
            this.parentPhone = phone;
            this.nextDigit = newDigit;
        }

        @Override
        protected String compute() {
            String currValue = this.parentPhone == null? "" :  this.parentPhone + String.valueOf(this.nextDigit);
            return currValue;

        }
    }
}
