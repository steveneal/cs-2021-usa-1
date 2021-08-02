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

    private String since;


    public VolumeTradedWithEntityExtractor() {

        this.since = DateTime.now().toString();

    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        //DateTime pastWeekMs = this.since.minusWeeks(1);
        //DateTime pasteMonthMs = this.since.minusMonths(1);
        //DateTime pastYearMs = this.since.minusYears(1);
        String pastWeekMs = DateTime.now().minusWeeks(1).toString().substring(0,10);
        String pastMonthMs = DateTime.now().minusMonths(1).toString().substring(0,10);
        String pastYearMs = DateTime.now().minusYears(1).toString().substring(0,10);

        String volumePastWeekQuery = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND TradeDate >= '%s'",
                //rfq.getTraderId(),
                rfq.getEntityId(),
                pastWeekMs);

        String volumePastMonthQuery = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND TradeDate >= '%s'",
                //rfq.getTraderId(),
                rfq.getEntityId(),
                pastMonthMs);

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
