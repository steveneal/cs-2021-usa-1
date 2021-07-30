package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.*;
import com.cs.rfq.decorator.publishers.MetadataJsonLogPublisher;
import com.cs.rfq.decorator.publishers.MetadataPublisher;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.joda.time.DateTime;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.spark.sql.functions.sum;

public class RfqProcessor {

    private final static Logger log = LoggerFactory.getLogger(RfqProcessor.class);

    private final SparkSession session;

    private final JavaStreamingContext streamingContext;

    private Dataset<Row> trades;

    private final List<RfqMetadataExtractor> extractors = new ArrayList<>();

    private final MetadataPublisher publisher = new MetadataJsonLogPublisher();



    public RfqProcessor(SparkSession session, JavaStreamingContext streamingContext) {
        this.session = session;
        this.streamingContext = streamingContext;

        //TODO: use the TradeDataLoader to load the trade data archives
        TradeDataLoader tdl = new TradeDataLoader();
        trades = tdl.loadTrades(session, "src\\test\\resources\\trades\\trades.json");

        //TODO: take a close look at how these two extractors are implemented
        extractors.add(new TotalTradesWithEntityExtractor());
        extractors.add(new VolumeTradedWithEntityYTDExtractor());
        extractors.add(new VolumeTradedWithEntityExtractor());
    }

    public void startSocketListener() throws InterruptedException {

        //TODO: stream data from the input socket on localhost:9000
        JavaDStream<String> lines = streamingContext.socketTextStream("localhost", 9000);
        JavaDStream<String> wordsp = lines.map(x -> x);
        wordsp.foreachRDD(rdd -> {
            rdd.collect().forEach(line -> consume(line));
        });
        //TODO: convert each incoming line to a Rfq object and call processRfq method with it
        Rfq  rfqobj = new Rfq();
        JavaDStream<Rfq> words = lines.map(x ->(rfqobj.fromJson(x)));
        words.foreachRDD(rdd -> {
            rdd.collect().forEach(line -> processRfq(line));
        });

        //TODO: start the streaming context
        streamingContext.start();
    }

    public void processRfq(Rfq rfq) {
        log.info(String.format("Received Rfq: %s", rfq.toString()));

        //create a blank map for the metadata to be collected
        Map<RfqMetadataFieldNames, Object> metadata = new HashMap<>();

        //TODO: get metadata from each of the extractors
        for (RfqMetadataExtractor extractor: extractors) {

         metadata.putAll(extractor.extractMetaData(rfq, session, trades));
            // extractor.extractMetaData(rfq, session, trades);
       }
        //TODO: publish the metadata
        publisher.publishMetadata(metadata);
    }

    static void consume(String line) {
        System.out.println(line);
    }
}
