package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;

public class IlliquidExtractor implements RfqMetadataExtractor {

    private String since;

    public IlliquidExtractor() {

        this.since = DateTime.now().toString();

    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        //use one year here but we could make it used as user define later
        String pastPeriod = DateTime.now().minusYears(1).toString().substring(0,10);

        String liquidityQuery = String.format("SELECT sum(LastQty) from trade where TraderId='%s' AND SecurityID='%s' AND TradeDate >= '%s'",
                rfq.getTraderId(),
                rfq.getIsin(),
                pastPeriod);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> volWeekResults = session.sql(liquidityQuery);

        Object liquidity = volWeekResults.first().get(0);

        if (liquidity == null) {
            liquidity = 0L;
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();

        results.put(illiquid, liquidity);

        return results;
    }
}
