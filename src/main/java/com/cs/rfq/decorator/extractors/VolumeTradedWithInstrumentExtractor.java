package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;

public class VolumeTradedWithInstrumentExtractor implements RfqMetadataExtractor {

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        String pastWeekMs = DateTime.now().minusWeeks(1).toString().substring(0,10);
        String pastMonthMs = DateTime.now().minusMonths(1).toString().substring(0,10);
        String pastYearMs = DateTime.now().minusYears(1).toString().substring(0,10);

        String volumePastWeekQuery = String.format("SELECT sum(LastQty) from trade where TraderId='%s' AND SecurityID='%s' AND TradeDate >= '%s'",
                rfq.getTraderId(),
                rfq.getIsin(),
                pastWeekMs);
        String volumePastMonthQuery = String.format("SELECT sum(LastQty) from trade where TraderId='%s' AND SecurityID='%s' AND TradeDate >= '%s'",
                rfq.getTraderId(),
                rfq.getIsin(),
                pastMonthMs);
        String volumePastYearQuery = String.format("SELECT sum(LastQty) from trade where TraderId='%s' AND SecurityID='%s' AND TradeDate >= '%s'",
                rfq.getTraderId(),
                rfq.getIsin(),
                pastYearMs);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> volTodayResults = session.sql(volumePastWeekQuery);
        Dataset<Row> volWeekResults = session.sql(volumePastMonthQuery);
        Dataset<Row> volYearResults = session.sql(volumePastYearQuery);

        Object volumeWeek = volTodayResults.first().get(0);
        Object volumeMonth = volWeekResults.first().get(0);
        Object volumeYear = volYearResults.first().get(0);
        if (volumeWeek == null) {
            volumeWeek = 0L;
        }
        if (volumeMonth == null) {
            volumeMonth = 0L;
        }
        if (volumeYear == null) {
            volumeYear = 0L;
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(instrumentVolumeTradedPastWeek, volumeWeek);
        results.put(instrumentVolumeTradedPastMonth, volumeMonth);
        results.put(instrumentVolumeTradedPastYear, volumeYear);
        return results;
    }

}
