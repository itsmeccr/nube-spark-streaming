package io.nubespark;

public class MainClass {

    public static void main(String[] args) throws InterruptedException {
        // todo get host, port, topic from args
        String host = "localhost";
        String port = "8181";
        String topic = "to-spark-streaming";

        NifiToFiloDbStreamer nifiToFiloDbStreamer = new NifiToFiloDbStreamer(host, port, topic);
        nifiToFiloDbStreamer.startStreaming();
    }
}
