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

        DateTime pastWeekMs = DateTime.now().minusWeeks(1);
        DateTime pasteMonthMs = DateTime.now().minusMonths(1);
        DateTime pastYearMs = DateTime.now().minusYears(1);

        String volumePastWeekQuery = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND TradeDate >= '%s'",
                //rfq.getTraderId(),
                rfq.getEntityId(),
                pastWeekMs);

        String volumePastMonthQuery = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND TradeDate >= '%s'",
                //rfq.getTraderId(),
                rfq.getEntityId(),
                pasteMonthMs);

        String volumePastYearQuery = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND TradeDate >= '%s'",
                //rfq.getTraderId(),
                rfq.getEntityId(),
                pastYearMs);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> volWeekResults = session.sql(volumePastWeekQuery);
        Dataset<Row> volMonthResults = session.sql(volumePastMonthQuery);
        Dataset<Row> volYearResults = session.sql(volumePastYearQuery);

        Object volumePastWeek = volWeekResults.first().get(0);
        Object volumePastMonth = volMonthResults.first().get(0);
        Object volumePastYear = volYearResults.first().get(0);

        if (volumePastWeek == null) {
            volumePastWeek = 0L;
        }
        if (volumePastMonth == null) {
            volumePastMonth = 0L;
        }
        if (volumePastYear == null) {
            volumePastYear = 0L;
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(entityVolumeTradedPastWeek, volumePastWeek);
        results.put(entityVolumeTradedPastMonth, volumePastMonth);
        results.put(entityVolumeTradedPastYear, volumePastYear);
        return results;
    }
}
