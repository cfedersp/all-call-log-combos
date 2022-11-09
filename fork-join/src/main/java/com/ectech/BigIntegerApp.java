package com.ectech;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.stream.Collectors;

/**
 * mvn clean package exec:exec
 *
 */
public class BigIntegerApp
{
    ForkJoinPool commonPool = ForkJoinPool.commonPool();
    public static void main( String[] args ) throws IOException {
        BigIntegerApp app = new BigIntegerApp();
        app.startCompute(1);
        // depth of 6 creates almost 6Mb
        // 7 creates 10M=70Mb
        // 8 creates 100M=800Mb
        // 9 creates 1000M=9Gb
        // 10 creates 10,000M= 100Gb
    }

    protected void startCompute(int depth) throws IOException {
        // Stream.of(1,2,3,4,5,6,7,8,9,10).map()
        // 1000000000L = 1 000 000 000
        // 100000000000000000
        // 1000000000000 = 100000 000 0000
        PhoneDepthLevelTask rt = new PhoneDepthLevelTask(BigInteger.valueOf(1000000000L), depth);
        List<BigInteger> generatedCombos = commonPool.invoke(rt);
        String fileName = "/tmp/combos-" + depth + ".txt";
        // Files.write(Paths.get(fileName), generatedCombos);

        File combosFile = Paths.get("/tmp/combos-" + depth + ".txt").toFile();

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(combosFile))) {
            for(BigInteger currPhone : generatedCombos) {
                bw.write(currPhone.toString() + "\n");
            }
        }


        System.out.println(String.format("Generated %d combos. written to: %s", generatedCombos.size(), fileName));
    }
    class PhoneDepthLevelTask extends RecursiveTask<List<BigInteger>> {
        protected BigInteger parentPhone;
        protected int nextDigit;
        protected int requiredDepth;
        protected int currentLevel;
        public PhoneDepthLevelTask(BigInteger phone, int requiredDepth) {
            this.parentPhone = phone;
            this.requiredDepth = requiredDepth;
            this.currentLevel = 0;
        }
        public PhoneDepthLevelTask(BigInteger phone, int newDigit, int requiredDepth, int currentLevel) {
            this.parentPhone = phone;
            this.nextDigit = newDigit;
            this.requiredDepth = requiredDepth;
            this.currentLevel = currentLevel;
        }

        protected List<PhoneDepthLevelTask> generateNextLevel(BigInteger currPhoneValue) {
            List<PhoneDepthLevelTask> phoneBuilders = new ArrayList<>();

            for (int i = 0; i < 10; i++) {
                phoneBuilders.add(new PhoneDepthLevelTask(currPhoneValue, i, requiredDepth, this.currentLevel+1));
            }
            return phoneBuilders;
        }
        @Override
        protected List<BigInteger> compute() {
            BigInteger currValue = this.parentPhone;
            if(this.currentLevel > 0)
                currValue = this.parentPhone.multiply(BigInteger.valueOf(10)).add(BigInteger.valueOf(this.nextDigit));
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
