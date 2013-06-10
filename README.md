Artisanal WordCount
=======================

Hadoop WordCount example revisited to illustrate how Dependency Injection can be used to reuse existing code in a
MapReduce context.

Artisanal WordCount imagines that there was a time before MapReduce when words were counted one at a time in small batches.
It illustrates how this might have been done using code components managed via an IoC container.

MapReduce WordCount is a variation on the Hadoop WordCount example that starts the same way as HadoopWordCount by
partitioning/sorting/shuffling an input file of words.  However, in the reduce phase, it uses the same core components used
by Artisanal WordCount by injecting MapReduce-friendly data sources and sinks.

Running the examples
-----

### Artisanal WordCount

    mvn compile exec:java \
    -Dexec.mainClass=com.opower.hadoop.examples.wordcount.artisanal.ArtisanalWordCount \
    -Dexec.args="data/input.txt target/output.artisanal.txt"

### MapReduce WordCount

    mvn compile exec:java \
    -Dexec.mainClass=com.opower.hadoop.examples.wordcount.mapreduce.MapReduceWordCount \
    -Dexec.args="data/input.txt target/output.mapreduce.txt"

