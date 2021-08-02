package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.AbstractSparkUnitTest;
import com.cs.rfq.decorator.extractors.RfqMetadataExtractor;
import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.cs.rfq.decorator.extractors.VolumeTradedWithEntityExtractor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;

public class VolumeTradedWithEntityExtractorTest extends  AbstractSparkUnitTest{

    Dataset<Row> trades;

    @BeforeEach
    public void setup() {
        String filePath = "src/test/resources/trades/simpleTrades2.json";
        trades = new TradeDataLoader().loadTrades(session, filePath);
    }

    @Test
    public void volumeEntityTest() {
        String validRfqJson = "{" +
                "'id': '123ABC', " +
                "'traderId': 5419847817764717882, " +
                "'entityId': 5561279226039690843, " +
                "'instrumentId': 'AT0000A001X2', " +
                "'qty': 250000, " +
                "'price': 1.58, " +
                "'side': 'B' " +
                "}";

        Rfq rfq = Rfq.fromJson(validRfqJson);
        Map<RfqMetadataFieldNames, Object> metadata = new HashMap<>();
        VolumeTradedWithEntityExtractor extractor = new VolumeTradedWithEntityExtractor();

        metadata.putAll(extractor.extractMetaData(rfq, session, trades));

        assertEquals((long) 850000, metadata.get(RfqMetadataFieldNames.entityVolumeTradedPastWeek));
        assertEquals((long) 850000, metadata.get(RfqMetadataFieldNames.entityVolumeTradedPastMonth));
        assertEquals((long) 1250000, metadata.get(RfqMetadataFieldNames.entityVolumeTradedPastYear));
    }


    @Test
    public void volumeEntityTest2() {
        String validRfqJson = "{" +
                "'id': '123ABC', " +
                "'traderId': 5419847817764717882, " +
                "'entityId': 5561279226039690847, " +
                "'instrumentId': 'AT0000A001X2', " +
                "'qty': 250000, " +
                "'price': 1.58, " +
                "'side': 'B' " +
                "}";

        Rfq rfq = Rfq.fromJson(validRfqJson);
        Map<RfqMetadataFieldNames, Object> metadata = new HashMap<>();
        VolumeTradedWithEntityExtractor extractor = new VolumeTradedWithEntityExtractor();

        metadata.putAll(extractor.extractMetaData(rfq, session, trades));

        assertEquals((long) 100000, metadata.get(RfqMetadataFieldNames.entityVolumeTradedPastWeek));
        assertEquals((long) 100000, metadata.get(RfqMetadataFieldNames.entityVolumeTradedPastMonth));
        assertEquals((long) 100000, metadata.get(RfqMetadataFieldNames.entityVolumeTradedPastYear));
    }
}
