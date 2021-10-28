package com.opstty;

import com.opstty.job.DistinctDistricts;
import com.opstty.job.Species;
import com.opstty.job.WordCount;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("distinctdistricts", DistinctDistricts.class,
                    "A map/reduce program that counts the distinct districts in the input file trees.csv.");
            programDriver.addClass("species", Species.class,
                    "A map/reduce program that lists the distinct species in the input file trees.csv.");
            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        System.exit(exitCode);
    }
}
