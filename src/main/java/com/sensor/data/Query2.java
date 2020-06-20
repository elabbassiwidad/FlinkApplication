package com.sensor.data;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;

import com.SensorData.*;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.sql.Timestamp;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class Query2 {
    public static void main(String[] args) throws Exception{

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(parameterTool);
        // we set the time characteristic to include an event in a window |event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Define Kafka Properties
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","kafka:9092");
        props.setProperty("schema.registry.url","schema-registry:8081");
        props.setProperty("group.id", "test");
        //String schemaRegistryUrl = parameterTool.getRequired("schema-registry-url");

        //1st query : Mall Foot Traffic-2 hours, each customer will be identified by it's Mac address

        DataStream<Zone1_Sensors> zone1 = env.addSource(new FlinkKafkaConsumer<>("1", ConfluentRegistryAvroDeserializationSchema.forSpecific(Zone1_Sensors.class, "http://schema-registry:8081") , props)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Zone1_Sensors>() {
                    @Override
                    public long extractAscendingTimestamp(Zone1_Sensors element) {
                        return element.getTimestamp();
                    }
                }));

        DataStream<Tuple2<String,Integer>> proximityTraffic = zone1.keyBy("Timestamp").window(TumblingProcessingTimeWindows.of(Time.minutes(30))).process(new org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<Zone1_Sensors, Tuple2<String,Integer>, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<Zone1_Sensors> input, Collector<Tuple2<String,Integer>> out) throws Exception {
                int count = 0;
                String area = new String();

                for (Zone1_Sensors in: input) {

                    if( in.getLatitude()<40.419643 && in.getLatitude()>=40.419373 ) {
                        if (in.getLongitude() < -3.705689 && in.getLongitude()>= -3.705999) {
                            count++;
                            area = "Zone1-Area1-Zara";
                        }
                    }
                    if( in.getLatitude()<=40.419890 && in.getLatitude()>40.419643 ) {
                        if (in.getLongitude() <= -3.705381 && in.getLongitude()> -3.705689) {
                            count++;
                            area = "Zone1-Area2-H&M";
                        }
                    }
                    if( in.getLatitude()<=40.419999 && in.getLatitude()>40.419890 ) {
                        if (in.getLongitude() <= -3.705075 && in.getLongitude()> -3.705689) {
                            count++;
                            area = "Zone1-Area3-Mango";
                        }
                    }
                }
                out.collect(new Tuple2<String,Integer>(area, count));
            }
        });

        // Control Visitors Traffic : if number of visitors exceeds 50 in some area, we Send an Advertisement to the Customers with Loyalty Programs or that dispose of The Mall Application

        // Send an Advertisement to the Customers with Loyalty Programs or that dispose of The Mall Application

        DataStream<Tuple3<Integer,String,String>> trafficJam = proximityTraffic.keyBy(0).window(TumblingProcessingTimeWindows.of(Time.minutes(20))).process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple3<Integer, String, String>, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> input, Collector<Tuple3<Integer, String, String>> out) throws Exception {

                Tuple3<Integer,String,String> traffic = new Tuple3<>();

                for (Tuple2<String,Integer> in: input) {
                    if ( in.f1 >=25 ) {
                        traffic.f0 = in.f1;
                        traffic.f1 = in.f0;
                        // Construct data
                        String apiKey = "apikey=" + "Tj/c/FveX0s-dRGHZGzi16ODdBjo4XKoiXRspYXetM";
                        String message = "&message=" + "Up to 70% Off on Kids Wear at H&M";
                        String sender = "&sender=" + "Mall Discounts";
                        String numbers = "&numbers=" + "34663514868";

                        // Send data
                        HttpURLConnection conn = (HttpURLConnection) new URL("https://api.txtlocal.com/send/?").openConnection();
                        String data = apiKey + numbers + message + sender;
                        conn.setDoOutput(true);
                        conn.setRequestMethod("POST");
                        conn.setRequestProperty("Content-Length", Integer.toString(data.length()));
                        conn.getOutputStream().write(data.getBytes("UTF-8"));
                        final BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                        final StringBuffer stringBuffer = new StringBuffer();
                        String line;
                        while ((line = rd.readLine()) != null) {
                            stringBuffer.append(line);
                        }
                        rd.close();

                        traffic.f2 = stringBuffer.toString();
                    }
                    else {
                        traffic.f0 = in.f1;
                        traffic.f1 = in.f0;
                        traffic.f2 = "No sign of Traffic Jam at this moment";
                    }
                }
                out.collect(new Tuple3<Integer,String,String>(traffic.f0,traffic.f1,traffic.f2));
            }
        });


        //ElasticSearch Sink Config

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("172.18.0.9", 9200, "http"));

        // create Index for category count
        ElasticsearchSink.Builder<Tuple2<String,Integer>> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Tuple2<String,Integer>>() {
                    public IndexRequest createIndexRequest(Tuple2<String,Integer> element) {
                        Map<String, Object> json = new HashMap<>();
                        json.put("Area", element.f0);
                        json.put("visitors", element.f1);

                        return Requests.indexRequest()
                                .index("proximity-traffic")
                                .type("proximity-traffic")
                                .source(json);
                    }

                    @Override
                    public void process(Tuple2<String,Integer> element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );
        esSinkBuilder.setBulkFlushMaxActions(1);
        proximityTraffic.addSink(esSinkBuilder.build());


        // create Index for category count
        ElasticsearchSink.Builder<Tuple3<Integer,String,String>> esSinkBuilder1 = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Tuple3<Integer,String,String>>() {
                    public IndexRequest createIndexRequest(Tuple3<Integer,String,String> element) {
                        Map<String, Object> json = new HashMap<>();
                        json.put("visitors", element.f0);
                        json.put("area", element.f1);
                        json.put("traffic observation", element.f2);


                        return Requests.indexRequest()
                                .index("traffic-jam")
                                .type("traffic-jam")
                                .source(json);
                    }

                    @Override
                    public void process(Tuple3<Integer,String,String> element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );
        esSinkBuilder1.setBulkFlushMaxActions(1);
        trafficJam.addSink(esSinkBuilder1.build());

        //proximityTraffic.writeAsText("/opt/flink/zone1");
        env.execute();

    }
}