package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.AbstractSparkUnitTest;
import com.cs.rfq.decorator.extractors.RfqMetadataExtractor;
import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.cs.rfq.decorator.extractors.VolumeTradedWithInstrumentExtractor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class VolumeTradedWithInstrumentExtractorTest extends  AbstractSparkUnitTest{
    Dataset<Row> trades;

    @BeforeEach
    public void setup() {
        String filePath = getClass().getResource("loader-test-trades.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
    }
//    get trades, make rfq value and call method and make sure returns correct volumes
    @Test
    public void volumeCheck() {
        String validRfqJson = "{" +
                "'id': '123ABC', " +
                "'traderId': 3351266293154445953, " +
                "'entityId': 5561279226039690843, " +
                "'instrumentId': 'AT0000383864', " +
                "'qty': 250000, " +
                "'price': 1.58, " +
                "'side': 'B' " +
                "}";
//   test  {'id': '123ABC', 'traderId': 3351266293154445953, 'entityId': 5561279226039690843, 'instrumentId': 'AT0000383864', 'qty': 250000, 'price': 1.58, 'side': 'B'}
        Rfq rfq = Rfq.fromJson(validRfqJson);
        Map<RfqMetadataFieldNames, Object> metadata = new HashMap<>();
        VolumeTradedWithInstrumentExtractor extractor = new VolumeTradedWithInstrumentExtractor();
        metadata.putAll(extractor.extractMetaData(rfq, session, trades));

    }
}
