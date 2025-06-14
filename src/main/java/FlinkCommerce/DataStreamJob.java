/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package FlinkCommerce;

import FlinkCommerce.deserializer.JSONValueDeserializationSchema;
import FlinkCommerce.dto.SalesPerCategory;
import FlinkCommerce.dto.SalesPerDay;
import FlinkCommerce.dto.SalesPerMonth;
import FlinkCommerce.dto.Transaction;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.FlushBackoffType;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;
import org.postgresql.xa.PGXADataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.util.function.Supplier;
import javax.sql.XADataSource;

import static FlinkCommerce.utils.JsonUtil.convertObjectToJson;

/**
 * a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {
    public static final String bootstrapServer = "localhost:9092";
    private static final String jdbcUrl = "jdbc:postgresql://localhost:5432/postgres";
    private static final String username = "postgres";
    private static final String password = "postgres";
    private static final Logger LOG = LoggerFactory.getLogger(DataStreamJob.class);


    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);

        final String topic = "product_transactions";

        KafkaSource<Transaction> source = KafkaSource.<Transaction>builder()
                .setBootstrapServers(bootstrapServer)
                .setTopics(topic)
                .setGroupId("flink-group")
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new JSONValueDeserializationSchema())
                .build();

        DataStream<Transaction> transactionStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSource");

        transactionStream.print();

        JdbcExecutionOptions execOptions = new JdbcExecutionOptions.Builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build();

        JdbcExecutionOptions zeroRetriesExecOptions = new JdbcExecutionOptions.Builder()
                .withMaxRetries(0)
                .build();

        JdbcExactlyOnceOptions exactlyOnceOptions = JdbcExactlyOnceOptions.builder()
                .withTransactionPerConnection(true)
                .build();

        JdbcConnectionOptions connOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(jdbcUrl)
                .withDriverName("org.postgresql.Driver")
                .withUsername(username)
                .withPassword(password).build();

        PGXADataSource xaDataSource = new org.postgresql.xa.PGXADataSource();
        xaDataSource.setUrl(jdbcUrl);
        xaDataSource.setUser(username);
        xaDataSource.setPassword(password);
        Supplier<XADataSource> dataSourceSupplier = () -> xaDataSource;

        // in postgres, create transaction table (if not exist)
        transactionStream.sinkTo(JdbcSink.<Transaction>builder()
                        .withQueryStatement(
                "CREATE TABLE IF NOT EXISTS transactions (" +
                        "transaction_id VARCHAR(255) PRIMARY KEY, " +
                        "product_id VARCHAR(255), " +
                        "product_name VARCHAR(255), " +
                        "product_category VARCHAR(255), " +
                        "product_price DOUBLE PRECISION, " +
                        "product_quantity INTEGER, " +
                        "product_brand VARCHAR(255), " +
                        "total_amount DOUBLE PRECISION, " +
                        "currency VARCHAR(255), " +
                        "customer_id VARCHAR(255), " +
                        "transaction_date TIMESTAMP, " +
                        "payment_method VARCHAR(255) " + ")", (preparedStatement, transaction) -> {})
                        .withExecutionOptions(execOptions)
                        .buildAtLeastOnce(connOptions))
                .name("Create Transaction Table Sink");

        // in postgres, create sales_per_category table
        transactionStream.sinkTo(JdbcSink.<Transaction>builder()
                .withQueryStatement(
                "CREATE TABLE IF NOT EXISTS sales_per_category (" +
                        "transaction_date DATE, " +
                        "category VARCHAR(255), " +
                        "total_sales DOUBLE PRECISION, " +
                        "PRIMARY KEY (transaction_date, category)" +
                        ")",
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {})
                .withExecutionOptions(execOptions)
                .buildAtLeastOnce(connOptions))
                .name("Create Sales Per Category Table");

        //create sales_per_day table sink
        transactionStream.sinkTo(JdbcSink.<Transaction>builder()
                .withQueryStatement(
                        "CREATE TABLE IF NOT EXISTS sales_per_day (" +
                        "transaction_date DATE PRIMARY KEY, " +
                        "total_sales DOUBLE PRECISION " +
                        ")",
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {})
                .withExecutionOptions(execOptions)
                .buildAtLeastOnce(connOptions))
                .name("Create Sales Per Day Table");

        //create sales_per_month table sink
        transactionStream.sinkTo(JdbcSink.<Transaction>builder()
                .withQueryStatement(
                "CREATE TABLE IF NOT EXISTS sales_per_month (" +
                        "year INTEGER, " +
                        "month INTEGER, " +
                        "total_sales DOUBLE PRECISION, " +
                        "PRIMARY KEY (year, month)" +
                        ")",
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {})
                .withExecutionOptions(execOptions)
                .buildAtLeastOnce(connOptions))
                .name("Create Sales Per Month Table");

        // upsert the transaction data
        // Exactly once is guaranteed by primary key as well as performing upsert by handling conflict
        // Flink's JDBC connector expects ? placeholders, not PostgreSQL's native $1, $2, $3... syntax
        transactionStream.sinkTo(JdbcSink.<Transaction>builder()
                .withQueryStatement(
                "INSERT INTO transactions(transaction_id, product_id, product_name, product_category, product_price, " +
                        "product_quantity, product_brand, total_amount, currency, customer_id, transaction_date, payment_method) " +
                        "VALUES (?, ?, ?,?, ?, ?,?, ?, ?,?, ?, ?) " +
                        "ON CONFLICT (transaction_id) DO UPDATE SET " +
                        "product_id = EXCLUDED.product_id, " +
                        "product_name  = EXCLUDED.product_name, " +
                        "product_category  = EXCLUDED.product_category, " +
                        "product_price = EXCLUDED.product_price, " +
                        "product_quantity = EXCLUDED.product_quantity, " +
                        "product_brand = EXCLUDED.product_brand, " +
                        "total_amount  = EXCLUDED.total_amount, " +
                        "currency = EXCLUDED.currency, " +
                        "customer_id  = EXCLUDED.customer_id, " +
                        "transaction_date = EXCLUDED.transaction_date, " +
                        "payment_method = EXCLUDED.payment_method ",

                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {
                    preparedStatement.setString(1, transaction.getTransactionId());
                    preparedStatement.setString(2, transaction.getProductId());
                    preparedStatement.setString(3, transaction.getProductName());
                    preparedStatement.setString(4, transaction.getProductCategory());
                    preparedStatement.setDouble(5, transaction.getProductPrice());
                    preparedStatement.setInt(6, transaction.getProductQuantity());
                    preparedStatement.setString(7, transaction.getProductBrand());
                    preparedStatement.setDouble(8, transaction.getTotalAmount());
                    preparedStatement.setString(9, transaction.getCurrency());
                    preparedStatement.setString(10, transaction.getCustomerId());
                    preparedStatement.setTimestamp(11, transaction.getTransactionDate());
                    preparedStatement.setString(12, transaction.getPaymentMethod());
                }).withExecutionOptions(execOptions)
                  .buildAtLeastOnce(connOptions))
        .name("Insert Transaction data");

        // upsert the sales_per_category data, grouping by category
        transactionStream.map(
                        transaction -> {
                            Date transactionDate = new Date(transaction.getTransactionDate().getTime());
                            String category = transaction.getProductCategory();
                            double totalSales = transaction.getTotalAmount();
                            return new SalesPerCategory(transactionDate, category, totalSales);
                        }
                ).keyBy(SalesPerCategory::getCategory)
                .reduce((salesPerCategory, t1) -> {
                    salesPerCategory.setTotalSales(salesPerCategory.getTotalSales() + t1.getTotalSales());
                    return salesPerCategory;
                }).sinkTo(JdbcSink.<SalesPerCategory>builder()
                        .withQueryStatement(
                        "INSERT INTO sales_per_category(transaction_date, category, total_sales) " +
                                "VALUES (?, ?, ?) " +
                                "ON CONFLICT (transaction_date, category) DO UPDATE SET " +
                                "total_sales = EXCLUDED.total_sales " +
                                "WHERE sales_per_category.category = EXCLUDED.category " +
                                "AND sales_per_category.transaction_date = EXCLUDED.transaction_date",
                                (JdbcStatementBuilder<SalesPerCategory>) (preparedStatement, salesPerCategory) -> {
                            preparedStatement.setDate(1, salesPerCategory.getTransactionDate());
                            preparedStatement.setString(2, salesPerCategory.getCategory());
                            preparedStatement.setDouble(3, salesPerCategory.getTotalSales());
                        })
                        .withExecutionOptions(execOptions)
                        .buildAtLeastOnce(connOptions))
                .name("update Category total");


        // upsert the sales_per_day data, grouping by day
        transactionStream.map(
                        transaction -> {
                            Date transactionDate = new Date(transaction.getTransactionDate().getTime());
                            double totalSales = transaction.getTotalAmount();
                            return new SalesPerDay(transactionDate, totalSales);
                        }
                ).keyBy(SalesPerDay::getTransactionDate)
                .reduce((salesPerDay, t1) -> {
                    salesPerDay.setTotalSales(salesPerDay.getTotalSales() + t1.getTotalSales());
                    return salesPerDay;
                }).sinkTo(JdbcSink.<SalesPerDay>builder()
                        .withQueryStatement(
                        "INSERT INTO sales_per_day(transaction_date, total_sales) " +
                                "VALUES (?, ?) " +
                                "ON CONFLICT (transaction_date) DO UPDATE SET " +
                                "total_sales = EXCLUDED.total_sales ",
                        (JdbcStatementBuilder<SalesPerDay>) (preparedStatement, salesPerDay) -> {
                            preparedStatement.setDate(1, salesPerDay.getTransactionDate());
                            preparedStatement.setDouble(2, salesPerDay.getTotalSales());
                        })
                        .withExecutionOptions(execOptions)
                        .buildAtLeastOnce(connOptions))
                .name("update sales_per_day total");

        // upsert the sales_per_month data, grouping by year, month
        transactionStream.map(
                        transaction -> {
                            Date transactionDate = new Date(transaction.getTransactionDate().getTime());
                            Integer year = transactionDate.toLocalDate().getYear();
                            Integer month = transactionDate.toLocalDate().getMonthValue();
                            double totalSales = transaction.getTotalAmount();
                            return new SalesPerMonth(year, month, totalSales);
                        }
                ).keyBy(new KeySelector<SalesPerMonth, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> getKey(SalesPerMonth salesPerMonth) throws Exception {
                        return new Tuple2<>(salesPerMonth.getMonth(), salesPerMonth.getYear());
                    }
                })
                .reduce((salesPerMonth, t1) -> {
                    salesPerMonth.setTotalSales(salesPerMonth.getTotalSales() + t1.getTotalSales());
                    return salesPerMonth;
                }).sinkTo(JdbcSink.<SalesPerMonth>builder()
                        .withQueryStatement(
                        "INSERT INTO sales_per_month(year, month, total_sales) " +
                                "VALUES (?, ?, ?) " +
                                "ON CONFLICT (year, month) DO UPDATE " +
                                "SET total_sales = EXCLUDED.total_sales ",
                        (JdbcStatementBuilder<SalesPerMonth>) (preparedStatement, salesPerMonth) -> {
                            preparedStatement.setInt(1, salesPerMonth.getYear());
                            preparedStatement.setInt(2, salesPerMonth.getMonth());
                            preparedStatement.setDouble(3, salesPerMonth.getTotalSales());
                        })
                        .withExecutionOptions(execOptions)
                        .buildAtLeastOnce(connOptions))
                .name("update sales_per_month total");

        // add to elastic search
        transactionStream.sinkTo(new Elasticsearch7SinkBuilder<Transaction>()
                        .setHosts(new HttpHost("localhost", 9200, "http"))
                        .setEmitter((transaction, runtimeContext, requestIndexer) -> {
                            LOG.info("emit transaction= {}", convertObjectToJson(transaction));
                            if (transaction != null && transaction.getTransactionId() != null) {
                                IndexRequest indexRequest = Requests.indexRequest()
                                        .index("transactions")
                                        .id(transaction.getTransactionId())
                                        .source(convertObjectToJson(transaction), XContentType.JSON);

                                LOG.info("indexReq= {}", indexRequest);
                                requestIndexer.add(indexRequest);
                            }
                        })
                        .setBulkFlushMaxActions(1)
                        .setBulkFlushMaxSizeMb(1)
                        .setBulkFlushInterval(5000)
                        .setBulkFlushBackoffStrategy(FlushBackoffType.EXPONENTIAL, 2, 1000)
                        .build())
                .name("Elasticsearch sink");

        // Execute program, beginning computation.
        env.execute("Flink Ecommerce Realtime Streaming");
    }
}
