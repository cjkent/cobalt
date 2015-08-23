package io.github.cjkent.cobalt;

import java.util.function.Supplier;

/**
 *
 */
public class Stopwatch {

    private Stopwatch() {
    }

    public static <T> T time(Supplier<T> body) {
        return time("Elapsed time", body);
    }

    public static <T> T time(String message, Supplier<T> body) {
        long start = System.currentTimeMillis();
        try {
            System.out.println("Starting stopwatch");
            return body.get();
        } finally {
            long time = System.currentTimeMillis() - start;
            System.out.println(message + " " + time + "ms");
        }
    }
}
