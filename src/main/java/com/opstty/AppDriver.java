package com.opstty;

import com.opstty.job.*;
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
            programDriver.addClass("speciescount", SpeciesCount.class,
                    "A map/reduce program that returns the distinct tree species (and the number of trees for each one) in the Remarkable Trees of Paris dataset.");
            programDriver.addClass("maxheight", MaxHeightTreeSpecies.class,
                    "A map/reduce program that returns the maximum height of each specific species of trees ");
            programDriver.addClass("treesheightsorter", HeightSorter.class,
                    "A map/reduce program that returns the trees sorted by height");
            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        System.exit(exitCode);
    }
}
