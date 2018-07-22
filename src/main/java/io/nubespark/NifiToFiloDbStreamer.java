package io.nubespark;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.nifi.spark.NiFiDataPacket;
import org.apache.nifi.spark.NiFiReceiver;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;

public class NifiToFiloDbStreamer {

    private String host;
    private String port;
    private String topic;


    public NifiToFiloDbStreamer(String host, String port, String topic) {
        this.host = host;
        this.port = port;
        this.topic = topic;
    }

    public static SparkConf getSparkConfig() {
        return new SparkConf()
                .setAppName("Streaming from Kafka to FiloDB")
                .setMaster("local[2]")
                .set("spark.executor.extraClassPath", "/opt/programs/spark-2.0.2-bin-hadoop2.6/conf/");
    }

    private static JavaStreamingContext createContext(String host, String port, String nifiTopic, String checkpointName) {

        SparkConf sparkConf = getSparkConfig();

        SiteToSiteClientConfig config = new SiteToSiteClient.Builder()
                .url("http://"+host+":"+port+"/nifi")
                .portName(nifiTopic)
                .buildConfig();

        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        jsc.checkpoint(checkpointName);

        // Create a JavaReceiverInputDStream using a NiFi receiver so that we can pull data from
        // specified Port
        JavaReceiverInputDStream<NiFiDataPacket> packetStream =
                jsc.receiverStream(new NiFiReceiver(config, StorageLevel.MEMORY_ONLY()));

        packetStream.checkpoint(Durations.seconds(60));

        // Map the data from NiFi to text, ignoring the attributes
        JavaDStream<String> text = packetStream.map((Function<NiFiDataPacket, String>) dataPacket -> new String(dataPacket.getContent(), StandardCharsets.UTF_8));

        SQLContext sqlContext = new SQLContext(jsc.sparkContext());
//        sqlContext.udf().register("timestampUDF", new TimestampUDF(), DataTypes.TimestampType);

        text.foreachRDD(rdd -> {
                    if (!rdd.isEmpty()) {
                        Dataset<Row> dataframe = sqlContext.read().json(rdd);
//                    dataframe = dataframe.withColumn("timestamp", callUDF("timestampUDF", col("datetime")));
                        dataframe.printSchema();
                        dataframe.show(1000);
                        dataframe.write()
                                .format("filodb.spark")
                                .option("dataset", "nifi_streaming_his_v1")
                                .mode(SaveMode.Append)
                                .save();
                    }
                    //todo transformers start here

                }
        );

        return jsc;
    }



    public void startStreaming() throws InterruptedException {

        String checkpointName = "sparkstreaming/checkpoint/" + topic;
        //This gives reference to function which creates context
        Function0<JavaStreamingContext> createContextFunc =
                () -> createContext(host, port, topic, checkpointName);
        //if no checkpoint is found. then it is first execution and createContextFunc is called
        //if checkpoint is found, state is retained from checkpoint
        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(checkpointName, createContextFunc);

        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                System.out.println("##############################################");
                System.out.println("Gracefully Shutting down spark streaming....");
                jsc.stop();
                System.out.println("Spark streaming has been shut down");
                System.out.println("##############################################");
            }
        });
        jsc.start();              // Start the computation
        jsc.awaitTermination();   // Wait for the computation to terminate
    }

    public static class TimestampUDF implements UDF1<Long, Timestamp> {

        @Override
        public Timestamp call(Long dateTime) throws Exception {
            return new Timestamp(dateTime / (1000 * 1000));
        }
    }




}
