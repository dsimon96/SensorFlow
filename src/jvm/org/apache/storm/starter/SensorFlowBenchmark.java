package org.apache.storm.starter;

import org.apache.commons.io.input.ReversedLinesFileReader;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

public class SensorFlowBenchmark {
    private final static int NUM_LINES = 100;
    public static double get(String name) {
        try {
            ReversedLinesFileReader reader = new ReversedLinesFileReader(new File("src/latency-" + name + ".txt"),
                    Charset.defaultCharset());

            Integer average = 0;

            for (int i = 0; i < NUM_LINES; i++) {
                String line = reader.readLine();
                if (line == null) {
                    return -1;
                }
                Integer millis = Integer.parseInt(line);

                average += millis;
            }

            return average.doubleValue() / Integer.valueOf(NUM_LINES).doubleValue();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return -1.0;
    }
}
