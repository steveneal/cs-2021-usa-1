package com.cs.rfq.decorator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.apache.spark.sql.types.DataTypes.*;

public class TradeDataLoader {

    private final static Logger log = LoggerFactory.getLogger(TradeDataLoader.class);

    public Dataset<Row> loadTrades(SparkSession session, String path) {

        //TODO: create an explicit schema for the trade data in the JSON files
        StructType schema = new StructType(new StructField[]{
                new StructField("TraderId", LongType, true, Metadata.empty()),
                new StructField("EntityId", LongType, true, Metadata.empty()),
                new StructField("SecurityID", StringType, true, Metadata.empty()),
                new StructField("LastQty", LongType, true, Metadata.empty()),
                new StructField("LastPx", DoubleType, true, Metadata.empty()),
                new StructField("TradeDate", DateType, true, Metadata.empty()),
                new StructField("Currency", StringType, true, Metadata.empty()),
                new StructField("Side", IntegerType, true, Metadata.empty()),
                new StructField("MsgType", IntegerType, true, Metadata.empty()),
                new StructField("TradeReportId", LongType, true, Metadata.empty()),
                new StructField("PreviouslyReported", StringType, true, Metadata.empty()),
                new StructField("SecurityIdSource", IntegerType, true, Metadata.empty()),
                new StructField("TransactTime", StringType, true, Metadata.empty()),
                new StructField("NoSides", IntegerType, true, Metadata.empty()),
                new StructField("OrderID", LongType, true, Metadata.empty()),
        });

        //TODO: load the trades dataset
        Dataset<Row> trades = session.read().schema(schema).json(path);

        //TODO: log a message indicating number of records loaded and the schema used
        log.info(String.format("Number of records loaded %d%n", trades.count()));
        log.info(schema.toString());
        return trades;
    }

}