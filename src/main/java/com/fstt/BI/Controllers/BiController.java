package com.fstt.BI.Controllers;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
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

	@GetMapping("/articles/year")
	public List<Tuple2<Object, Object>> articlesYear() {
		SparkSession spark = SparkSession.builder().master("local").appName("MongoSparkConnectorIntro")
				.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/db1.articles")
				.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/db1.articles").getOrCreate();

		// Declare the Schema via a Java Bean
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		Dataset<Row> explicitDF = MongoSpark.load(jsc).toDF(Article.class);
		explicitDF.printSchema();

		// SQL
		// explicitDF.registerTempTable("articles");

		Dataset<Row> articles = spark.sql("SELECT count(latitude), date_pub FROM articles GROUP BY date_pub");
		List<Tuple2<Object, Object>> listArticles = articles.toJavaRDD()
				.map(row -> new Tuple2<>(row.get(1), row.get(0))).collect();
		return listArticles;
	}

	@GetMapping("/articles/country")
	public List<Tuple2<Object, Object>> articlesCountry() {
		SparkSession spark = SparkSession.builder().master("local").appName("MongoSparkConnectorIntro")
				.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/db1.articles")
				.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/db1.articles").getOrCreate();

		// Declare the Schema via a Java Bean
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		Dataset<Row> explicitDF = MongoSpark.load(jsc).toDF(Article.class);
		// explicitDF.printSchema();

		// SQL
		explicitDF.registerTempTable("articles");

		Dataset<Row> articles = spark.sql("SELECT count(latitude), country FROM articles GROUP BY country");
		List<Tuple2<Object, Object>> listArticles = articles.toJavaRDD()
				.map(row -> new Tuple2<>(row.get(1), row.get(0))).collect();
		return listArticles;
	}

	@GetMapping("/wordscount")
	public List<Tuple2<String, Integer>> wordsCount() {
		SparkSession spark = SparkSession.builder().master("local").appName("MongoSparkConnectorIntro")
				.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/db1.articles")
				.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/db1.articles").getOrCreate();

		// Declare the Schema via a Java Bean
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		Dataset<Row> explicitDF = MongoSpark.load(jsc).toDF(Article.class);
		//explicitDF.printSchema();

		// SQL
		explicitDF.registerTempTable("articles");

		Dataset<Row> titles = spark.sql("SELECT title FROM articles where title != \"\"");
		
		List<Tuple2<String, Integer>> counts = titles.toJavaRDD()
				.flatMap(s -> Arrays.asList(((String) s.get(0)).toLowerCase()
						.replace(":","")
						.replaceAll("\\b( ?'s|an|of|to|a|and|in|that|the|from|under|for|with|on)\\b", "").split(" ")).iterator())
				.mapToPair(word -> new Tuple2<>(word, 1))
				.reduceByKey((a, b) -> a + b)
				.mapToPair(x -> x.swap())
				.sortByKey(false)
				.mapToPair(x -> x.swap())
				.take(50);
		return counts;
	}

	private class TupleComparator implements Comparator<Tuple2<String, Integer>>, Serializable {
		@Override
		public int compare(Tuple2<String, Integer> tuple1, Tuple2<String, Integer> tuple2) {
			return tuple1._2 < tuple2._2 ? 0 : 1;
		}
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
