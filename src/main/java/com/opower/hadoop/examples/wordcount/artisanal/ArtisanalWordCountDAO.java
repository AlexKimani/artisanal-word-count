package com.opower.hadoop.examples.wordcount.artisanal;

import com.google.common.collect.AbstractIterator;
import com.opower.hadoop.examples.wordcount.WordCountDAO;
import com.opower.hadoop.examples.wordcount.WordCountDTO;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;

/**
 * DAO that linearly scans through a file using a single thread to count word frequencies.
 */
public class ArtisanalWordCountDAO implements WordCountDAO {
    private final File inputFile;
    private final File outputFile;

    private static final class WordFileScanningIterable implements Iterable<String> {
        private BufferedReader wordFileReader;

        WordFileScanningIterable(File wordFile) throws Exception {
            this.wordFileReader = new BufferedReader(new FileReader(wordFile));
        }

        @Override
        public Iterator<String> iterator() {
            return new AbstractIterator<String>() {
                @Override
                protected String computeNext() {
                    try {
                        String nextWord = wordFileReader.readLine();
                        if (nextWord != null) {
                            return nextWord;
                        }
                        else {
                            wordFileReader.close();
                            return endOfData();
                        }
                    }
                    catch (IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                }
            };
        }
    }

    public ArtisanalWordCountDAO(File inputFile, File outputFile) {
        this.inputFile = inputFile;
        this.outputFile = outputFile;
    }

    @Override
    public Iterable<String> getWords() throws Exception {
        return new WordFileScanningIterable(this.inputFile);
    }

    public void writeWordCount(WordCountDTO wordCount) throws Exception {
        Writer writer = new FileWriter(this.outputFile, true);
        writer.write(String.format("%s\t%d\n", wordCount.getWord(), wordCount.getCount()));
        writer.flush();
        writer.close();
    }
}
