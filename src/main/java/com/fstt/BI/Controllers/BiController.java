package com.fstt.BI.Controllers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fstt.BI.Service.ArticleService;
import com.fstt.BI.model.Article;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;

import scala.Tuple2;

@RestController
public class BiController {

	@GetMapping("/api/articles/")
	public void articlesCountry() {
		SparkSession spark = SparkSession.builder().master("local").appName("MongoSparkConnectorIntro")
				.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/db1.articles")
				.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/db1.articles").getOrCreate();

		// Declare the Schema via a Java Bean
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        Dataset<Row> explicitDF = MongoSpark.load(jsc).toDF(Article.class);
        explicitDF.printSchema();

        // SQL
        explicitDF.registerTempTable("articles");

	 
	        
	        Dataset<Row> teenagers = spark.sql("SELECT count(latitude), date_pub FROM articles GROUP BY date_pub");
	        List<String> teenagerNames = teenagers.toJavaRDD()
	            .map((Row row) -> "{" + row.get(1)+": "+ row.get(0) +"}").collect();
	        System.out.println(teenagerNames);

	}

	public void test() {
		SparkSession spark = SparkSession.builder().master("local").appName("MongoSparkConnectorIntro")
				.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/db1.articles")
				.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/db1.articles").getOrCreate();

		// Create a JavaSparkContext using the SparkSession's SparkContext object
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		Map<String, String> readOverrides = new HashMap<String, String>();
		readOverrides.put("collection", "articles");
		ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

		JavaRDD<Document> customRdd = MongoSpark.load(jsc, readConfig);

		/*
		 * JavaRDD<Document> aggregatedRdd = customRdd.withPipeline( singletonList(
		 * Document.parse("{ $match: { test : { $gt : 5 } } }")));
		 * 
		 * JavaPairRDD<String, Integer> counts = textFile .flatMap(s ->
		 * Arrays.asList(s.split(" ")).iterator()) .mapToPair(word -> new Tuple2<>(word,
		 * 1)) .reduceByKey((a, b) -> a + b);
		 */
		// counts.saveAsTextFile("hdfs://...");
		// System.out.println(customRdd.count());
		// System.out.println(customRdd.count());

		Dataset<Row> explicitDF = MongoSpark.load(jsc).toDF(Article.class);
		// SQL
		explicitDF.registerTempTable("articles");

		Dataset<Row> abstracts = explicitDF.select("abstract_");
		// customRdd.foreach((VoidFunction<Document>) row ->
		// System.out.println(row.get("abstract_")));

		JavaPairRDD<String, Integer> counts = customRdd
				.flatMap((FlatMapFunction<Document, String>) s -> Arrays
						.asList(((String) s.get("abstract_")).split(" ")).iterator())
				.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((a, b) -> a + b);

		// counts.foreach((VoidFunction<Tuple2<String, Integer>>) tuple ->
		// System.out.println(tuple.productPrefix() + "->" + tuple.productArity()));

		/*
		 * JavaPairRDD<String, Integer> counts = JavaPairRDD<String, Integer> counts =
		 * textFile .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
		 * .mapToPair(word -> new Tuple2<>(word, 1)) .reduceByKey((a, b) -> a + b);
		 * 
		 * //System.out.println(abstracts); /*JavaPairRDD<String, Integer> counts =
		 * abstracts .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
		 * .mapToPair(word -> new Tuple2<>(word, 1)) .reduceByKey((a, b) -> a + b);
		 */
	}
}
