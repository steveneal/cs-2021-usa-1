package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;

public class VolumeTradedWithEntityExtractor implements RfqMetadataExtractor {

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        DateTime todayMs = DateTime.now();
        DateTime pastWeekMs = DateTime.now().minusWeeks(1);
        DateTime pastYearMs = DateTime.now().minusYears(1);

        String volumeTodayQuery = String.format("SELECT sum(LastQty) from trade where TraderId='%s' AND SecurityID='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(),
                rfq.getIsin(),
                todayMs);
        String volumePastWeekQuery = String.format("SELECT sum(LastQty) from trade where TraderId='%s' AND SecurityID='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(),
                rfq.getIsin(),
                pastWeekMs);
        String volumePastYearQuery = String.format("SELECT sum(LastQty) from trade where TraderId='%s' AND SecurityID='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(),
                rfq.getIsin(),
                pastYearMs);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> volTodayResults = session.sql(volumeTodayQuery);
        Dataset<Row> volWeekResults = session.sql(volumePastWeekQuery);
        Dataset<Row> volYearResults = session.sql(volumePastYearQuery);

        Object volumeToday = volTodayResults.first().get(0);
        Object volumeWeek = volWeekResults.first().get(0);
        Object volumeYear = volYearResults.first().get(0);
        if (volumeToday == null) {
            volumeToday = 0L;
        }
        if (volumeWeek == null) {
            volumeWeek = 0L;
        }
        if (volumeYear == null) {
            volumeYear = 0L;
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(instruentVolumeTradedToday, volumeToday);
        results.put(instrumentVolumeTradedPastWeek, volumeWeek);
        results.put(instrumentVolumeTradedPastYear, volumeYear);
        return results;
    }
}
