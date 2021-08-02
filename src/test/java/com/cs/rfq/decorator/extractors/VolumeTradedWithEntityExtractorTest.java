package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import com.cs.rfq.utils.ChatterboxServer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class VolumeTradedWithEntityExtractorTest extends AbstractSparkUnitTest {
    Dataset<Row> trades;

    @BeforeEach
    public void setup() {
        String filePath = getClass().getResource("loader-test-trades.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
    }

    @Test
    public void loadTradeRecords() {
        assertEquals(5, trades.count());
    }


   @Test
   public void testJSonFactoryMethod() {
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


   }

    @Test
   public void testConn() {

       ChatterboxServer service = new ChatterboxServer();

   }



}
