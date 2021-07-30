package com.cs.rfq.decorator;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.mortbay.log.Log;
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
                new StructField("SecurityId", StringType, true, Metadata.empty()),
                new StructField("LastQty", LongType, true, Metadata.empty()),
                new StructField("LastPx", DoubleType, true, Metadata.empty()),
                new StructField("Side", IntegerType, true, Metadata.empty()),
                new StructField("MsgType", IntegerType, true, Metadata.empty()),
                new StructField("TradeReportId", LongType, true, Metadata.empty()),
                new StructField("PreviouslyReported", StringType, true, Metadata.empty()),
                new StructField("SecurityIdSource", IntegerType, true, Metadata.empty()),
                new StructField("TradeDate", DateType, true, Metadata.empty()),
                new StructField("TransactTime", TimestampType, true, Metadata.empty()),
                new StructField("NoSides", IntegerType, true, Metadata.empty()),
                new StructField("OrderID", LongType, true, Metadata.empty()),
                new StructField("Currency", StringType, true, Metadata.empty())

        });

//        'TraderId':5419847817764717882, 'EntityId':5561279226039690843, 'MsgType':35, 'TradeReportId':3141628235479330982, 'PreviouslyReported':'N', 'SecurityID':'AT0000A001X2', 'SecurityIdSource':4, 'LastQty':100000, 'LastPx':112.023, 'TradeDate':'2020-07-30', 'TransactTime':'20200730-13:14:52', 'NoSides':1, 'Side':1, 'OrderID':3138272914859569503, 'Currency':'EUR'}

        //TODO: load the trades dataset
        Dataset<Row> trades = session.read().schema(schema).json(path);

        //TODO: log a message indicating number of records loaded and the schema used
        long loaded = trades.count();
        log.info(String.format("Number of records loaded %d%n", loaded));
        Log.info(schema.toString());

        return trades;
    }

}

