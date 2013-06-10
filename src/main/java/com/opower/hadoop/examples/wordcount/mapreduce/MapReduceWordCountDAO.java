package com.opower.hadoop.examples.wordcount.mapreduce;

import com.google.common.collect.AbstractIterator;
import com.opower.hadoop.examples.wordcount.WordCountDAO;
import com.opower.hadoop.examples.wordcount.WordCountDTO;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.Iterator;

/**
 * Implementation of {@link WordCountDAO} that "streams" a {@link WordCountDTO} from
 * {@link org.apache.hadoop.mapreduce.Mapper#map(Object, Object, org.apache.hadoop.mapreduce.Mapper.Context)} parameters.
 */
public class MapReduceWordCountDAO implements WordCountDAO {
    private final String word;
    private final int wordCount;
    private final Reducer.Context reduceContext;

    /**
     * Produces an iterator that repeats back the {@link #word} specified at construction {@link #wordCount} times.
     */
    private static final class WordRepeatingIterable implements Iterable<String> {
        private final String word;
        private int wordCount;

        WordRepeatingIterable(String word, int wordCount) {
            this.word = word;
            this.wordCount = wordCount;
        }

        @Override
        public Iterator<String> iterator() {
            return new AbstractIterator<String>() {
                @Override
                protected String computeNext() {
                    if (wordCount > 0) {
                        --wordCount;
                        return word;
                    }
                    else {
                        return endOfData();
                    }
                }
            };
        }
    }

    MapReduceWordCountDAO(Text key, Iterable<IntWritable> values, Reducer.Context reduceContext) {
        this.word = key.toString();
        this.reduceContext = reduceContext;

        int valuesSum = 0;
        for (IntWritable value : values) {
            valuesSum += value.get();
        }
        this.wordCount = valuesSum;
    }

    @Override
    public Iterable<String> getWords() throws Exception {
        return new WordRepeatingIterable(this.word, this.wordCount);
    }

    @Override
    public void writeWordCount(WordCountDTO wordCount) throws Exception {
        this.reduceContext.write(new Text(wordCount.getWord()), new IntWritable(wordCount.getCount()));
    }
}
