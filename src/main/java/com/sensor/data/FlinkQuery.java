package com.sensor.data;

import com.SensorData.SensorData;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.Properties;

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
        props.setProperty("group.id", "test");
        //String schemaRegistryUrl = parameterTool.getRequired("schema-registry-url");

        //1st query : Mall Foot Traffic-2 hours, each customer will be identified by it's Mac address

        DataStream<SensorData> mallFootTraffic = env.addSource(new FlinkKafkaConsumer<>("Sensor-Data", ConfluentRegistryAvroDeserializationSchema.forSpecific(SensorData.class, "http://schema-registry:8081") , props));
        mallFootTraffic.writeAsText("/opt/flink/test");
        env.execute();


        //   mallFootTraffic.addSink(new FlinkKafkaProducer<SensorData>("mallFootTraficHistory", new ObjSerializationSchema("mall_FootTrafic_History"),
        //         props,FlinkKafkaProducer.Semantic.EXACTLY_ONCE));

    }

}
