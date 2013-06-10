package com.opower.hadoop.examples.wordcount;

/**
 * Data access object for retrieving word occurrences and writing a word count data transfer object
 */
public interface WordCountDAO {
    /**
     * @return an iterable that returns all words retrieved from the word data source
     */
    public Iterable<String> getWords() throws Exception;

    /**
     * Write a {@link WordCountDTO} to a persistent store (e.g. a file)
     */
    public void writeWordCount(WordCountDTO wordCount) throws Exception;

}
