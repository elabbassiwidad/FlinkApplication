# Flink Application purpose

The goal of this Project is to build a Streaming pipeline Using **Kafka** and **Flink**.

Mock data is generated using [kafka-connect datagen](https://github.com/confluentinc/kafka-connect-datagen).
Flink query results will be pushed back to kafka, stored In HBase or HDFS.

# Scenario 

Multiple Sensors/BLE beacons/Wifi Location tracking are placed through tha Mall to track Customers as they move through shops, and each 
customer has the Mall App running in their phones, the app will then receive signals via Bluethooth and also respond sending the 
customer's data back : Mac Address, IP Address, Timestamp, longitude and latitude. At the same time, Another flow of Customer Transactions
will be also sent from the POS. 

This continuous flow of sensor readings will be first pushed to Kafka, and then ingested to Flink to run customized queries on it.
The insighits we like to get from this data are the following :
  
- we'de like to quantify the customer’s journey and purchase pattern,
- trigger the Customer App with adds/offers of shops nearby.
- Mall Foot Traffic, Proximity Traffic, %Capture Rate, Store Sales conversion.
- item boughts at each shop, previous visits and a history of customers will be kept(as part of loyalty programs).
- Identify the under-performing areas, to come up with boosting strategy.
  ....
