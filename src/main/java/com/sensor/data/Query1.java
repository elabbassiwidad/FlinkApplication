package com.sensor.data;

import com.SensorData.*;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
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
        // we set the time characteristic to include a processing time window
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // Define Kafka Properties
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","kafka:9092");
        props.setProperty("schema.registry.url","schema-registry:8081");
        props.setProperty("group.id", "RetailProject");

        //1st query : Mall Foot Traffic-2 hours, each customer will be identified by it's Mac address
        //Zone1

       DataStream<Zone1_Sensors> zone1 = env.addSource(new FlinkKafkaConsumer<>("1", ConfluentRegistryAvroDeserializationSchema.forSpecific(Zone1_Sensors.class, "http://schema-registry:8081") , props));
       DataStream<Tuple3<String,Long,Integer>> mapped = zone1.map(new MapFunction<Zone1_Sensors, Tuple3<String, Long, Integer>>() {
           @Override
           public Tuple3<String,Long, Integer> map(Zone1_Sensors value) throws Exception {

               return new Tuple3<String,Long, Integer>(value.getSensorUID(),  value.getTimestamp(), 1);
           }
       });
       DataStream<Tuple3<String, Long, Integer>> resultzone1 = mapped
               .keyBy(0)
               .reduce(new ReduceFunction<Tuple3<String, Long, Integer>>() {
           @Override
           public Tuple3<String, Long, Integer> reduce(Tuple3<String, Long, Integer> value1, Tuple3<String, Long, Integer> value2) throws Exception {
               return new Tuple3<String, Long, Integer>(value1.f0,value1.f1,value1.f2+value2.f2);
           }
       });

       //Zone2
        DataStream<Zone2_Sensors> zone2 = env.addSource(new FlinkKafkaConsumer<>("2", ConfluentRegistryAvroDeserializationSchema.forSpecific(Zone2_Sensors.class, "http://schema-registry:8081") , props));
        DataStream<Tuple3<String,Long,Integer>> mapped2 = zone2.map(new MapFunction<Zone2_Sensors, Tuple3<String, Long, Integer>>() {
            @Override
            public Tuple3<String,Long, Integer> map(Zone2_Sensors value) throws Exception {

                return new Tuple3<String,Long, Integer>(value.getSensorUID(),  value.getTimestamp(), 1);
            }
        });
        DataStream<Tuple3<String, Long, Integer>> resultzone2 = mapped2
                .keyBy(0)
                .reduce(new ReduceFunction<Tuple3<String, Long, Integer>>() {
                    @Override
                    public Tuple3<String, Long, Integer> reduce(Tuple3<String, Long, Integer> value1, Tuple3<String, Long, Integer> value2) throws Exception {
                        return new Tuple3<String, Long, Integer>(value1.f0,value1.f1,value1.f2+value2.f2);
                    }
                });

        //Zone3
        DataStream<Zone3_Sensors> zone3 = env.addSource(new FlinkKafkaConsumer<>("3", ConfluentRegistryAvroDeserializationSchema.forSpecific(Zone3_Sensors.class, "http://schema-registry:8081") , props));
        DataStream<Tuple3<String,Long,Integer>> mapped3 = zone3.map(new MapFunction<Zone3_Sensors, Tuple3<String, Long, Integer>>() {
            @Override
            public Tuple3<String,Long, Integer> map(Zone3_Sensors value) throws Exception {

                return new Tuple3<String,Long, Integer>(value.getSensorUID(),  value.getTimestamp(), 1);
            }
        });
        DataStream<Tuple3<String, Long, Integer>> resultzone3 = mapped3
                .keyBy(0)
                .reduce(new ReduceFunction<Tuple3<String, Long, Integer>>() {
                    @Override
                    public Tuple3<String, Long, Integer> reduce(Tuple3<String, Long, Integer> value1, Tuple3<String, Long, Integer> value2) throws Exception {
                        return new Tuple3<String, Long, Integer>(value1.f0,value1.f1,value1.f2+value2.f2);
                    }
                });

        //Joining streams of the different zones

        DataStream<Tuple3<Long,Integer,Integer>> firstStream = resultzone1.join(resultzone2).where(
                new KeySelector<Tuple3<String,Long,Integer>, Long>() {
                    @Override
                    public Long getKey(Tuple3<String,Long,Integer> value) throws Exception {
                        return value.f1;
                    }
                }
        ).equalTo(
                new KeySelector<Tuple3<String,Long,Integer>, Long>() {
                    @Override
                    public Long getKey(Tuple3<String,Long,Integer> value) throws Exception {
                        return value.f1;
                    }
                }
        ).window(TumblingProcessingTimeWindows.of(Time.minutes(10)))
                .apply(new JoinFunction<Tuple3<String,Long,Integer>, Tuple3<String,Long,Integer>, Tuple3<Long,Integer,Integer>>()
                {
                    @Override
                    public Tuple3<Long, Integer, Integer> join(Tuple3<String, Long, Integer> zone1, Tuple3<String, Long, Integer> zone2) throws Exception {
                        return new Tuple3<Long, Integer, Integer>(zone1.f1,zone1.f2,zone2.f2);
                    }
                });

      DataStream<Tuple4<Long,Integer,Integer,Integer>> joinedStream = firstStream.join(resultzone3).where(
                      new KeySelector<Tuple3<Long,Integer,Integer>, Long>() {
                          @Override
                          public Long getKey(Tuple3<Long,Integer,Integer> value) throws Exception {
                              return value.f0;
                          }
                      }
              ).equalTo(
                      new KeySelector<Tuple3<String,Long,Integer>, Long>() {
                          @Override
                          public Long getKey(Tuple3<String,Long,Integer> value) throws Exception {
                              return value.f1;
                          }
                      }
              ).window(TumblingProcessingTimeWindows.of(Time.minutes(20))).apply(
              new JoinFunction<Tuple3<Long,Integer,Integer>, Tuple3<String,Long,Integer>, Tuple4<Long,Integer,Integer,Integer>>()
              {
                  @Override
                  public Tuple4<Long,Integer,Integer,Integer> join(Tuple3<Long,Integer,Integer> zone1_2, Tuple3<String, Long, Integer> zone3) throws Exception {
                      return new Tuple4<Long,Integer,Integer,Integer>(zone1_2.f0,zone1_2.f1,zone1_2.f2,zone3.f2);
                  }
              });


       //ElasticSearch Sink Config

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("172.18.0.4", 9200, "http"));

        ElasticsearchSink.Builder<Tuple4<Long,Integer,Integer,Integer>> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Tuple4<Long,Integer,Integer,Integer>>() {
                    public IndexRequest createIndexRequest(Tuple4<Long,Integer,Integer,Integer> element) {
                        Map<String, Object> json = new HashMap<>();
                        json.put("timestamp", element.f0);
                        json.put("visitors_Zone1", element.f1);
                        json.put("visitors_Zone2", element.f2);
                        json.put("visitors_Zone3", element.f3);

                        return Requests.indexRequest()
                                .index("flink-streams")
                                .type("flink-stream")
                                .source(json);
                    }

                    @Override
                    public void process(Tuple4<Long,Integer,Integer,Integer> element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );
        esSinkBuilder.setBulkFlushMaxActions(1);
        joinedStream.addSink(esSinkBuilder.build());

        env.execute();
    }

}
