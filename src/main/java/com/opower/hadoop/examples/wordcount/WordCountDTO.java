package com.opower.hadoop.examples.wordcount;

/**
 * Transfer object for holding a word and a corresponding frequency count.
 */
public class WordCountDTO {
    private final String word;
    private final int count;

    public WordCountDTO(String word, int count) {
        this.word = word;
        this.count = count;
    }

    public String getWord() {
        return this.word;
    }

    public int getCount() {
        return this.count;
    }
}
