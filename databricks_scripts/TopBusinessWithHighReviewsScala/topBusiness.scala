import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val columnNamesB = Seq("_c0", "_c2", "_c4")
val columnNamesR = Seq("_c4", "_c6")
var df1 = spark.read.format("csv").option("header", "false").option("delimiter", ":").load("/FileStore/tables/user.csv")
var df2 = spark.read.format("csv").option("header", "false").option("delimiter", ":").load("/FileStore/tables/business.csv")
var df3 = spark.read.format("csv").option("header", "false").option("delimiter", ":").load("/FileStore/tables/review.csv")
var df2_filtered = df2.dropDuplicates("_c0")
df2_filtered = df2_filtered.filter(df2_filtered("_c2").contains(" NY "))
df2_filtered = df2_filtered.select(columnNamesB.head, columnNamesB.tail: _*)
var df3_filtered = df3.select(columnNamesR.head, columnNamesR.tail: _*)
val df_asBusiness = df2_filtered.as("business")
val df_asReview = df3_filtered.as("review")

var joined_df = df_asReview.join(
    df_asBusiness, df_asBusiness("_c0") === df_asReview("_c4"), "inner").drop(df_asReview("_c4"))
joined_df.withColumn("_c6", $"_c6".cast(DoubleType))
var final_df = joined_df.groupBy("_c0", "_c2", "_c4").agg(avg("_c6").as("AverageRating"))
final_df = final_df.orderBy(final_df("AverageRating").desc, final_df("_c0").desc).limit(10)
final_df.collect.foreach(println)