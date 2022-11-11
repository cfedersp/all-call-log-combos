package com.ectech;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * mvn clean package exec:exec
 * Next version will use RecursiveAction and a ConcurrentLinkedQueue to write directly to a file
 */
public class InMemoryApp
{
    ForkJoinPool commonPool = ForkJoinPool.commonPool();
    public static void main( String[] args ) throws IOException {
        InMemoryApp app = new InMemoryApp();
        LocalDateTime dt = LocalDateTime.now();
        app.startCompute(7);
        Duration dur = Duration.between(dt, LocalDateTime.now());
        System.out.println(String.format("Completed in %d:%02d:%02d", dur.toHoursPart(), dur.toMinutesPart(), dur.toSecondsPart()));
        // depth of 6 creates almost 6Mb
        // 7 creates 9 999 999 ~10M ~70Mb
        // 8 creates 100M=800Mb
        // 9 creates 1000M=9Gb
        // 10 creates 10,000M= 100Gb

        // each worker creates 10 entries, so 10M entries is created by 1M workers.
    }

    protected void startCompute(int depth) throws IOException {
        // Stream.of(1,2,3,4,5,6,7,8,9,10).map()

        PhoneDepthLevelTask rt = new PhoneDepthLevelTask(depth);
        List<String> generatedCombos = commonPool.invoke(rt);
        List<String> filteredCombos = generatedCombos.stream()
            //.filter(s -> s.length() == depth)
            //.filter(s -> !s.startsWith("0"))
            .collect(Collectors.toList());
        String fileName = "/tmp/combos-" + depth + ".txt";
        Files.write(Paths.get(fileName), filteredCombos);
        /*
        File combosFile = Paths.get("/tmp/combos-" + depth + ".txt").toFile();

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(combosFile))) {
            for(BigInteger currPhone : filteredCombos) {
                bw.write(currPhone.toString() + "\n");
            }
        }

         */
        System.out.println(String.format("Generated %d combos. remaining %d, written to: %s", generatedCombos.size(), filteredCombos.size(), fileName));
    }
    class PhoneDepthLevelTask extends RecursiveTask<List<String>> {
        protected String parentPhone;
        protected int nextDigit;
        protected int requiredDepth;
        protected int currentLevel;
        public PhoneDepthLevelTask(int requiredDepth) {
            this.requiredDepth = requiredDepth;
            this.currentLevel = 0;
        }
        public PhoneDepthLevelTask(String phone, int newDigit, int requiredDepth, int currentLevel) {
            this.parentPhone = phone;
            this.nextDigit = newDigit;
            this.requiredDepth = requiredDepth;
            this.currentLevel = currentLevel;
        }

        protected List<PhoneDepthLevelTask> generateNextLevel(String currPhoneValue) {
            return Stream.iterate(0, n -> n + 1)
                .limit(10).map(i -> new PhoneDepthLevelTask(currPhoneValue, i, requiredDepth, this.currentLevel+1)).collect(Collectors.toList());

        }
        @Override
        protected List<String> compute() {
            String currValue = this.parentPhone == null? "" :  this.parentPhone + String.valueOf(this.nextDigit);
            if(this.currentLevel < this.requiredDepth) {
                List<PhoneDepthLevelTask> nextLevel = generateNextLevel(currValue);
                return ForkJoinTask.invokeAll(nextLevel).stream().map(ForkJoinTask::join).flatMap(o -> o.stream())
                    .collect(Collectors.toList());

            } else {
                return List.of(currValue);
            }
        }
    }
}
