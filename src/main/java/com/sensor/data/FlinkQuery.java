package com.sensor.data;

import com.SensorData.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import java.util.Date;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.*;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlinkQuery {
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
        props.setProperty("group.id", "RetailProject");

        //1st query : Mall Foot Traffic-2 hours, each customer will be identified by it's Mac address

       DataStream<Zone1_Sensors> mallFootTraffic = env.addSource(new FlinkKafkaConsumer<>("Zone1_Data", ConfluentRegistryAvroDeserializationSchema.forSpecific(Zone1_Sensors.class, "http://schema-registry:8081") , props)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Zone1_Sensors>() {
                    @Override
                    public long extractAscendingTimestamp(Zone1_Sensors element) {
                        return element.getTimestamp();
                    }
                }));
       DataStream<Tuple3<String,Long,Integer>> mapped = mallFootTraffic.map(new MapFunction<Zone1_Sensors, Tuple3<String, Long, Integer>>() {
           @Override
           public Tuple3<String,Long, Integer> map(Zone1_Sensors value) throws Exception {

               return new Tuple3<String,Long, Integer>(value.getSensorUID(),  value.getTimestamp(), 1);
           }
       });
       DataStream<Tuple3<String, Long, Integer>> result = mapped
               .keyBy(0)
               .window(TumblingEventTimeWindows.of(Time.minutes(20)))
               .reduce(new ReduceFunction<Tuple3<String, Long, Integer>>() {
           @Override
           public Tuple3<String, Long, Integer> reduce(Tuple3<String, Long, Integer> value1, Tuple3<String, Long, Integer> value2) throws Exception {
               return new Tuple3<String, Long, Integer>(value1.f0,value1.f1,value1.f2+value2.f2);
           }
       });
       //ElasticSearch Sink Config
        Map<String, String> config = new HashMap<>();
        config.put("cluster.name", "docker-cluster");
        // the sink will emit after every element, otherwise they would be buffered
        config.put("bulk.flush.max.actions", "1");

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("172.18.0.7", 9200, "http"));

        ElasticsearchSink.Builder<Tuple3<String, Long, Integer>> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Tuple3<String, Long, Integer>>() {
                    public IndexRequest createIndexRequest(Tuple3<String, Long, Integer> element) {
                        Map<String, Object> json = new HashMap<>();
                        json.put("sensorID", element.f0);
                        json.put("timestamp", element.f1);
                        json.put("visitorsCount", element.f2);

                        return Requests.indexRequest()
                                .index("flink-test")
                                .type("flink-log")
                                .source(json);
                    }

                    @Override
                    public void process(Tuple3<String, Long, Integer> element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );
        esSinkBuilder.setBulkFlushMaxActions(1);
        result.addSink(esSinkBuilder.build());

        env.execute();
    }

}
