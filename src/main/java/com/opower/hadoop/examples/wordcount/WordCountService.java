package com.opower.hadoop.examples.wordcount;

/**
 * Business interface for counting the occurrences of a specified word.  Implementations are expected to both perform the
 * count calculation and write the results.
 */
public interface WordCountService {
    void countWord(String word);
}
