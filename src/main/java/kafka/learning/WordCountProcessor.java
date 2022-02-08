package kafka.learning;

import java.util.Arrays;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class WordCountProcessor {

    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
    	
        KStream<String, String> messageStream = streamsBuilder
            .stream("groupby-count-input", Consumed.with(STRING_SERDE, STRING_SERDE));

        /*
        KTable<String, String> fdsgdf = messageStream
            .mapValues((ValueMapper<String, String>) String::toLowerCase)
            .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
            .groupBy((key, word) -> word, Grouped.with(STRING_SERDE, STRING_SERDE))
            .count(Materialized.as("counts"))
            .mapValues((k,v) -> String.valueOf(v));
        */
        
        KTable<String, String> wordCounts = messageStream
                .mapValues((k,v) -> v.toLowerCase())
                .mapValues((k,v) -> v.split("\\W+"))
                .mapValues((k,v) -> Arrays.asList(v))
                .flatMapValues((k,v) -> v)
                .groupBy((key, word) -> word, Grouped.with(STRING_SERDE, STRING_SERDE))
                .count(Materialized.as("counts"))
                .mapValues((k,v) -> String.valueOf(v));

        wordCounts.toStream().to("groupby-count-output");
    }

}