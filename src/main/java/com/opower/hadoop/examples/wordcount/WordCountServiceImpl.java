package com.opower.hadoop.examples.wordcount;

import com.google.inject.Inject;

/**
 * Service that computes word counts by interacting with a {@link WordCountDAO}.  This class is agnostic with regards to
 * whether it is being called in an "Artisanal" or a MapReduce processing context.
 */
public class WordCountServiceImpl implements WordCountService {
    private WordCountDAO wordCountDAO;

    public void countWord(String word) {
        try {
            int wordCount = 0;
            for(String nextWord : this.wordCountDAO.getWords()) {
                if (nextWord.equals(word)) {
                    ++wordCount;
                }
            }
            this.wordCountDAO.writeWordCount(new WordCountDTO(word, wordCount));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Inject
    public void setWordCountDAO(WordCountDAO wordCountDAO) {
        this.wordCountDAO = wordCountDAO;
    }
}
