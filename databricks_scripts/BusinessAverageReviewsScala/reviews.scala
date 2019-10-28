val columnNamesB = Seq("_c0")
val columnNamesR = Seq("_c2", "_c4", "_c6")
var df1 = spark.read.format("csv").option("header", "false").option("delimiter", ":").load("/FileStore/tables/user.csv")
var df2 = spark.read.format("csv").option("header", "false").option("delimiter", ":").load("/FileStore/tables/business.csv")
var df3 = spark.read.format("csv").option("header", "false").option("delimiter", ":").load("/FileStore/tables/review.csv")
var df2_filtered = df2.dropDuplicates("_c0")
df2_filtered = df2_filtered.filter(df2_filtered("_c4").contains("Colleges & Universities, Education"))
df2_filtered = df2_filtered.select(columnNamesB.head, columnNamesB.tail: _*)
var df3_filtered = df3.select(columnNamesR.head, columnNamesR.tail: _*)
val df_asBusiness = df2_filtered.as("business")
val df_asReview = df3_filtered.as("review")

var joined_df = df_asReview.join(
    df_asBusiness, df_asBusiness("_c0") === df_asReview("_c4"), "inner").drop(df_asReview("_c4"))

var final_df = joined_df.drop("_c0")
final_df.collect.foreach(println)
final_df.write.format("com.databricks.spark.csv").option("header", "true").save("/FileStore/tables/reviews")