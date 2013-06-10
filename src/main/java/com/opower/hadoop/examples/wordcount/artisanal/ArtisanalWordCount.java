package com.opower.hadoop.examples.wordcount.artisanal;

import com.google.inject.Guice;
import com.opower.hadoop.examples.wordcount.WordCountDAO;
import com.opower.hadoop.examples.wordcount.WordCountGuiceModule;
import com.opower.hadoop.examples.wordcount.WordCountService;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Collection;
import java.util.TreeSet;

/**
 * Calculates word count by scanning an entire file one row at a time.
 */
public class ArtisanalWordCount {
    private int run(File inputFile, File outputFile) throws Exception {
        WordCountDAO wordCountDAO = new ArtisanalWordCountDAO(inputFile, outputFile);

        WordCountService wordCountService =
                Guice.createInjector(new WordCountGuiceModule(wordCountDAO))
                        .getInstance(WordCountService.class);

        for (String word : getDistinctWordsInFile(inputFile)) {
            wordCountService.countWord(word);
        }

        return 0;
    }

    /**
     * It is nonsense that we would scan through a file and gather distinct words but *not* count occurrences at the
     * same time.  But for purposes of illustration, we need a source of words from which we can call
     * {@link WordCountService#countWord(String)}.
     */
    private Collection<String> getDistinctWordsInFile(File inputFile) throws Exception {
        Collection<String> words = new TreeSet<String>();
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        for (String line = reader.readLine(); line != null; line = reader.readLine()) {
            words.add(line);
        }
        reader.close();
        return words;
    }

    public static void main(String args[]) throws Exception {
        File inputFile = new File(args[0]);
        File outputFile = new File(args[1]);
        // clear previous output file contents before running
        outputFile.delete();

        new ArtisanalWordCount().run(inputFile, outputFile);
    }

}
