# Kafka Connector with Snowflake 


## Pre-Requisit:
  - Install kafka
  - download the ```snowflake-kafka-connector.jar``` and put inside ```/usr/local/opt/kafka/libexec/libs/```
  - update ```connect-standalone.properties``` with ```plugin.path=/usr/local/opt/kafka/libexec/libs/```
  - Create an unencrypted private key
    > openssl genrsa -out rsa_key.pem 2048
  - Create a public key referencing the above private key
    > openssl rsa -in rsa_key.pem -pubout -out rsa_key.pub
  - Create and Update private key inside ```SF_connect.properties``` as ```snowflake.private.key```
    ```properties
      connector.class=com.snowflake.kafka.connector.SnowflakeSinkConnector
      tasks.max=8
      topics=sales-data
      snowflake.topic2table.map=sales-data:sales_data
      buffer.count.records=10000
      buffer.flush.time=60
      buffer.size.bytes=5000000
      snowflake.url.name=xkshnvm-xz97051.snowflakecomputing.com
      snowflake.user.name=pjaiswal
      snowflake.private.key=<<private_key>>
      snowflake.database.name=DEMO_DB
      snowflake.schema.name=PUBLIC
      key.converter=com.snowflake.kafka.connector.records.SnowflakeJsonConverter
      value.converter=com.snowflake.kafka.connector.records.SnowflakeJsonConverter
      name=kafka_live_streaming_data
    ```
  - put ```SF_connect.properties``` inside ```/usr/local/etc/kafka```
  - Update Snowflake user with public key:
    ```sql
        alter user pjaiswal set RSA_PUBLIC_KEY=<<public_key>>
    ```
    
## Kafka steps 
- Start Zookeeper :
   ```shell
    sh /usr/local/opt/kafka/bin/zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties
   ```
- Start Kafka Server:
  ```shell
    sh /usr/local/opt/kafka/bin/kafka-server-start /usr/local/etc/kafka/server.properties
  ```
- Create Kafka Topic:
  ```shell
    sh /usr/local/opt/kafka/bin/kafka-topics --create --topic sales-data --bootstrap-server localhost:9092
  ```
- connecting Kafka with Snowflake systems
  ```shell
    sh /usr/local/opt/kafka/bin/connect-standalone /usr/local/etc/kafka/connect-standalone.properties /usr/local/etc/kafka/SF_connect.properties
  ```
- Post message into kafka topic
  ```shell
    sh /usr/local/opt/kafka/bin/kafka-console-producer -broker-list localhost:9092 -topic sales-data
  ```
- Retrieve Data in Snowflake
  ```sql
    SELECT RECORD_CONTENT:age::INT as age, RECORD_CONTENT:car::STRING as Car, RECORD_CONTENT:name::STRING as name
    FROM DEMO_DB.PUBLIC.SALES_DATA;
  ```

###  Delete a topic 
  ```shell
    sh /usr/local/opt/kafka/bin/kafka-topics --bootstrap-server localhost:9092 --delete --topic test-data
  ```

 ###  list all topics 
  ```shell
    sh /usr/local/opt/kafka/bin/kafka-topics --list --bootstrap-server localhost:9092
  ```

### Read message from Kafka topic
```shell
  sh /usr/local/opt/kafka/bin/kafka-console-consumer --topic sales-data --from-beginning --bootstrap-server localhost:9092
```


