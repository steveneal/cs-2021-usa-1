package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.AbstractSparkUnitTest;
import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.cs.rfq.decorator.extractors.VolumeTradedWithInstrumentExtractor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;

public class VolumeTradedWithInstrumentExtractorTest extends  AbstractSparkUnitTest{
    Dataset<Row> trades;

    @BeforeEach
    public void setup() {
        String filePath = "src/test/resources/trades/simpleTrades.json";
        trades = new TradeDataLoader().loadTrades(session, filePath);
    }
//    get trades, make rfq value and call method and make sure returns correct volumes
    @Test
    public void volumeCheckSameWeekMonthValues() {
        String validRfqJson = "{" +
                "'id': '123ABC', " +
                "'traderId': 5419847817764717882, " +
                "'entityId': 5561279226039690843, " +
                "'instrumentId': 'AT0000A001X2', " +
                "'qty': 250000, " +
                "'price': 1.58, " +
                "'side': 'B' " +
                "}";
//   test  {'id': '123ABC', 'traderId': 3351266293154445953, 'entityId': 5561279226039690843, 'instrumentId': 'AT0000383864', 'qty': 250000, 'price': 1.58, 'side': 'B'}
        Rfq rfq = Rfq.fromJson(validRfqJson);
        Map<RfqMetadataFieldNames, Object> metadata = new HashMap<>();
        VolumeTradedWithInstrumentExtractor extractor = new VolumeTradedWithInstrumentExtractor();
        metadata.putAll(extractor.extractMetaData(rfq, session, trades));
        assertAll(
                () -> assertEquals((long) 950000, metadata.get(RfqMetadataFieldNames.instrumentVolumeTradedPastWeek)),
                () -> assertEquals((long) 950000, metadata.get(RfqMetadataFieldNames.instrumentVolumeTradedPastMonth)),
                () -> assertEquals((long) 1350000, metadata.get(RfqMetadataFieldNames.instrumentVolumeTradedPastYear))
        );
    }
    @Test
    public void volumeCheckDifferentWeekMonthYearValues() {
        String validRfqJson = "{" +
                "'id': '123ABC', " +
                "'traderId': 6915717929522265936, " +
                "'entityId': 5561279226039690843, " +
                "'instrumentId': 'AT0000A0U3T4', " +
                "'qty': 250000, " +
                "'price': 1.58, " +
                "'side': 'B' " +
                "}";
        Rfq rfq = Rfq.fromJson(validRfqJson);
        Map<RfqMetadataFieldNames, Object> metadata = new HashMap<>();
        VolumeTradedWithInstrumentExtractor extractor = new VolumeTradedWithInstrumentExtractor();
        metadata.putAll(extractor.extractMetaData(rfq, session, trades));
        assertAll(
                () -> assertEquals((long) 100000, metadata.get(RfqMetadataFieldNames.instrumentVolumeTradedPastWeek)),
                () -> assertEquals((long) 350000, metadata.get(RfqMetadataFieldNames.instrumentVolumeTradedPastMonth)),
                () -> assertEquals((long) 600000, metadata.get(RfqMetadataFieldNames.instrumentVolumeTradedPastYear))
        );
    }
}
