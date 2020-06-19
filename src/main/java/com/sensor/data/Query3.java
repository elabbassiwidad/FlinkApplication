package com.sensor.data;

import com.SensorData.PurchaseData;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.windowing.*;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.text.SimpleDateFormat;
import java.util.*;

public class Query3 {
    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(parameterTool);
        // we set the time characteristic to include an event in a specific window
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // Define Kafka Properties
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "kafka:9092");
        props.setProperty("schema.registry.url", "schema-registry:8081");
        props.setProperty("group.id", "RetailProject");

        //1st query : Mall Foot Traffic-2 hours, each customer will be identified by it's Mac address
        //

        DataStream<PurchaseData> purchaseStream = env.addSource(new FlinkKafkaConsumer<>("POS1", ConfluentRegistryAvroDeserializationSchema.forSpecific(PurchaseData.class, "http://schema-registry:8081"), props));
        DataStream<PurchaseData> purchaseStream2 = env.addSource(new FlinkKafkaConsumer<>("POS2", ConfluentRegistryAvroDeserializationSchema.forSpecific(PurchaseData.class, "http://schema-registry:8081"), props));

        // Defining Under-performing Categories
        // in this Type of Queries, it would be better if we use Session Windows, each sale will have a time window to be done

        DataStream<Tuple3<String,String,Integer>> categoryCount = purchaseStream2.keyBy("Product_Category").window(TumblingProcessingTimeWindows.of(Time.minutes(15))).process(new org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<PurchaseData, Tuple3<String, String, Integer>, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<PurchaseData> input, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                int count = 0;
                String category = new String() ;
                String date = new String() ; ;
                //SimpleDateFormat dateTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                for (PurchaseData in: input) {
                    count++;
                    category = in.getProductCategory();
                    date = new java.text.SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(new java.util.Date ((in.getTimestamp())*1000));
                }
                out.collect(new Tuple3<String,String,Integer>(category,date, count));
            }
        });

        // Most Used Payment_Method
        DataStream<Tuple2<String,Integer>> paymentMethod = purchaseStream.keyBy("Payment_Method").window(TumblingProcessingTimeWindows.of(Time.minutes(15))).process(new org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<PurchaseData, Tuple2<String, Integer>, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<PurchaseData> input, Collector<Tuple2<String, Integer>> out) throws Exception {
                int count = 0;
                String payment = new String() ;
                for (PurchaseData in: input) {
                    count++;
                    payment= in.getPaymentMethod();
                }
                out.collect(new Tuple2<String,Integer>(payment, count));
            }
        });

        // Inventory Checking
        DataStream<Tuple3<Long,Integer,String>> stockAvailability = purchaseStream
                .keyBy("productid")
                .timeWindow(Time.minutes(15))
                .process(new org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<PurchaseData, Tuple3<Long, Integer, String>, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<PurchaseData> elements, Collector<Tuple3<Long, Integer, String>> out) throws Exception {
                        Tuple3<Long, Integer, String> result = new Tuple3<>();

                        String critical = "Item is below the System Quantity, needs REORDERING";
                        String available = "Item Quantity is sufficient";

                        for (PurchaseData in : elements) {
                            if (in.getNumberAvailableStock() < 30) {
                                result.f0 = in.getProductid();
                                result.f1 = in.getNumberAvailableStock();
                                result.f2 = critical;
                            } else {
                                result.f0 = in.getProductid();
                                result.f1 = in.getNumberAvailableStock();
                                result.f2 = available;
                            }
                        }
                        out.collect(new Tuple3<Long, Integer, String>(result.f0,result.f1,result.f2));
                    }
                });
        //Total sales
        DataStream<Tuple2<String,Integer>> totalSales = purchaseStream2.keyBy("Product_Category").timeWindow(Time.minutes(30)).process(new ProcessWindowFunction<PurchaseData, Tuple2<String, Integer>, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<PurchaseData> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
                int totalSales = 0;
              //  SimpleDateFormat dateTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String  date = new String() ;

                for (PurchaseData in: elements) {
                    totalSales=in.getAmount();
                    date = new java.text.SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(new java.util.Date ((in.getTimestamp())*1000));
                }
                out.collect(new Tuple2<String, Integer>(date, totalSales));
            }
        });

        //ElasticSearch Sink Config

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("172.18.0.4", 9200, "http"));

        // create Index for category count
        ElasticsearchSink.Builder<Tuple3<String,String,Integer>> esSinkBuilder1 = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Tuple3<String,String,Integer>>() {
                    public IndexRequest createIndexRequest(Tuple3<String,String,Integer> element) {
                        Map<String, Object> json = new HashMap<>();
                        json.put("category", element.f0);
                        json.put("date", element.f1);
                        json.put("total_Sales", element.f2);

                        return Requests.indexRequest()
                                .index("product-categories")
                                .type("product-categories")
                                .source(json);
                    }

                    @Override
                    public void process(Tuple3<String,String,Integer> element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );
        esSinkBuilder1.setBulkFlushMaxActions(1);
        categoryCount.addSink(esSinkBuilder1.build());

        // create Index for Payment Method
        ElasticsearchSink.Builder<Tuple2<String,Integer>> esSinkBuilder2 = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Tuple2<String,Integer>>() {
                    public IndexRequest createIndexRequest(Tuple2<String,Integer> element) {
                        Map<String, Object> json = new HashMap<>();
                        json.put("payment_Method", element.f0);
                        json.put("count", element.f1);

                        return Requests.indexRequest()
                                .index("store-payment")
                                .type("store-payment")
                                .source(json);
                    }

                    @Override
                    public void process(Tuple2<String,Integer> element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );
        esSinkBuilder2.setBulkFlushMaxActions(1);
        paymentMethod.addSink(esSinkBuilder2.build());

        // create Index for Stock Availability
        ElasticsearchSink.Builder<Tuple3<Long,Integer,String>> esSinkBuilder3 = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Tuple3<Long,Integer,String>>() {
                    public IndexRequest createIndexRequest(Tuple3<Long,Integer,String> element) {
                        Map<String, Object> json = new HashMap<>();
                        json.put("product_ID", element.f0);
                        json.put("stock_check", element.f1);
                        json.put("observation", element.f2);

                        return Requests.indexRequest()
                                .index("stock-availability")
                                .type("stock-availab")
                                .source(json);
                    }

                    @Override
                    public void process(Tuple3<Long,Integer,String> element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );
        esSinkBuilder3.setBulkFlushMaxActions(1);
        stockAvailability.addSink(esSinkBuilder3.build());

        // creating Index for Total Sales
        ElasticsearchSink.Builder<Tuple2<String,Integer>> esSinkBuilder4 = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Tuple2<String,Integer>>() {
                    public IndexRequest createIndexRequest(Tuple2<String,Integer> element) {
                        Map<String, Object> json = new HashMap<>();
                        json.put("date", element.f0);
                        json.put("total_Sales", element.f1);

                        return Requests.indexRequest()
                                .index("store-sales")
                                .type("store-sale")
                                .source(json);
                    }

                    @Override
                    public void process(Tuple2<String,Integer> element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );
        esSinkBuilder4.setBulkFlushMaxActions(1);
        totalSales.addSink(esSinkBuilder4.build());

        env.execute();

    }
}
