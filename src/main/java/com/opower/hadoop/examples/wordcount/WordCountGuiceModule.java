package com.opower.hadoop.examples.wordcount;

import com.google.inject.AbstractModule;

/**
 * Sets up dependency injection using Guice.
 * {@link WordCountServiceImpl} is the shared code component that can be used across implementations.
 */
public class WordCountGuiceModule extends AbstractModule {
    private final WordCountDAO wordCountDAO;

    public WordCountGuiceModule(WordCountDAO wordCountDAO) {
        this.wordCountDAO = wordCountDAO;
    }

    @Override
    protected void configure() {
        bind(WordCountService.class).to(WordCountServiceImpl.class);
        bind(WordCountDAO.class).toInstance(this.wordCountDAO);
    }
}
