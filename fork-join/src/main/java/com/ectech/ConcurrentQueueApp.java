package com.ectech;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
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
public class ConcurrentQueueApp
{
    ForkJoinPool commonPool = ForkJoinPool.commonPool();
    public static void main( String[] args ) throws IOException {
        System.out.println("Starting ConcurrentQueueApp");
        ConcurrentQueueApp app = new ConcurrentQueueApp();
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
        // ConcurrentLinkedQueue<String> lq = new ConcurrentLinkedQueue<>();
        // we're creating 10^(depth-1) workers and a single writer. Queue length should be at least 10^(depth-2)
        // Otherwise we'll have hundreds of blocked workers.
        LinkedBlockingQueue<QueueItem<String>> lq = new LinkedBlockingQueue(100000);
        RecursiveAction notifierAction = new QueueNotifierAction(lq, new PhoneDepthLevelTask(depth, lq));
        RecursiveAction writerTask = new WriteBlockingQueueToFileOutputTask(fileName, lq);
        RecursiveAction dagRoot = new DagRoot(writerTask, notifierAction);


        commonPool.invoke(dagRoot);


        // System.out.println(String.format("Generated %d combos. remaining %d, written to: %s", generatedCombos.size(), filteredCombos.size(), fileName));
    }
    class WriteNonBlockingQueueToFileOutputTask extends RecursiveAction {

        private String fileName;
        private Queue<String> lq;
        WriteNonBlockingQueueToFileOutputTask(String fileName, ConcurrentLinkedQueue<String> lq) {
            this.fileName = fileName;
            this.lq = lq;
        }
        @Override
        protected void compute() {
            File combosFile = Paths.get(fileName).toFile();
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(combosFile))) {
                while (lq.peek() != null) {
                    bw.write(lq.poll());
                }
            } catch (IOException e) {
                System.out.println(e.toString());
                throw new RuntimeException(e);
            }
        }
    }
    class QueueItem<T> {
        protected T item;
        protected boolean isPastTheEnd;

        public QueueItem(T item, boolean isPastTheEnd) {
            this.item = item;
            this.isPastTheEnd = isPastTheEnd;
        }

        public T getItem() {
            return item;
        }

        public void setItem(T item) {
            this.item = item;
        }

        public boolean isPastTheEnd() {
            return isPastTheEnd;
        }

        public void setPastTheEnd(boolean pastTheEnd) {
            isPastTheEnd = pastTheEnd;
        }
    }
    class DagRoot extends RecursiveAction {
        List<RecursiveAction> workerActions;
        public DagRoot(RecursiveAction writerAction, RecursiveAction writerNotifierWithWorkers) {
            workerActions = List.of(writerAction, writerNotifierWithWorkers);
        }

        @Override
        protected void compute() {
            ForkJoinTask.invokeAll(workerActions);
        }
    }
    class QueueNotifierAction extends RecursiveAction {
        private RecursiveAction rootWorker;
        private Queue q;
        public QueueNotifierAction(Queue q, RecursiveAction rootWorker) {
            this.q = q;
            this.rootWorker = rootWorker;
        }

        @Override
        protected void compute() {
            ForkJoinTask.invokeAll(rootWorker);
            System.out.println("finished workers. sending null to queue");
            q.add(new QueueItem<String>(null, true));
        }
    }
    class WriteBlockingQueueToFileOutputTask extends RecursiveAction {

        private String fileName;
        private BlockingQueue<QueueItem<String>> lq;
        WriteBlockingQueueToFileOutputTask(String fileName, BlockingQueue<QueueItem<String>> lq) {
            this.fileName = fileName;
            this.lq = lq;
        }
        @Override
        protected void compute() {
            File combosFile = Paths.get(fileName).toFile();
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(combosFile))) {
                /*
                while (lq.peek() != null) {
                    bw.write(lq.poll());
                }
                */
                QueueItem<String> item;
                while (true) {
                    item = lq.take();
                    if(item.isPastTheEnd()) {
                        System.out.println("finished writing all elements. breaking.");
                        break;
                    }
                    bw.write(item.getItem() + '\n');
                }
            } catch (InterruptedException e) {
                System.out.println(e.toString());
                throw new RuntimeException(e);
            } catch (IOException e) {
                System.out.println(e.toString());
                throw new RuntimeException(e);
            }
            System.out.println("finished writing output");
        }
    }

    /**
     * Note that as structured, this class cannot be generalized such that the DAG generation is independent of the compute.
     * To separate DAG generation from compute, state must be encapsulated.
     */
    class PhoneDepthLevelTask extends RecursiveAction {
        protected Queue<QueueItem<String>> lq;
        protected String parentPhone;
        protected int nextDigit;
        protected int requiredDepth;
        protected int currentLevel;
        public PhoneDepthLevelTask(int requiredDepth, Queue<QueueItem<String>> lq) {
            this.requiredDepth = requiredDepth;
            this.currentLevel = 0;
            this.lq = lq;
        }
        public PhoneDepthLevelTask(String phone, int newDigit, int requiredDepth, int currentLevel, Queue<QueueItem<String>> lq) {
            this.parentPhone = phone;
            this.nextDigit = newDigit;
            this.requiredDepth = requiredDepth;
            this.currentLevel = currentLevel;
            this.lq = lq;
        }

        protected List<RecursiveAction> generateNextLevel(String currPhoneValue) {
            return Stream.iterate(0, n -> n + 1)
                .limit(10).map(i -> new PhoneDepthLevelTask(currPhoneValue, i, requiredDepth, this.currentLevel+1, this.lq)).collect(Collectors.toList());

        }
        @Override
        protected void compute() {
            String currValue = this.parentPhone == null? "" :  this.parentPhone + String.valueOf(this.nextDigit);
            if(this.currentLevel < this.requiredDepth) {
                List<RecursiveAction> nextLevel = generateNextLevel(currValue);
                ForkJoinTask.invokeAll(nextLevel); // Dont try to interact with child Actions after this line or deadlock will occur.

            } else {
                lq.add(new QueueItem<String>(currValue, false));
            }
        }
    }
}
