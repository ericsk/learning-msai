// Databricks notebook source
// MAGIC %md #Bike Rental Analysis
// MAGIC 
// MAGIC **資料**: 資料來自於 [Capital Bikeshare](https://www.capitalbikeshare.com/) 系統 2011 - 2012 年的資料，並且加上了相關的欄位，例如：天氣。這份資料集由 Fanaee-T 與 Gama (2013) 整理，並且由 [UCI Machine Learning Repository](http://archive.ics.uci.edu/ml/datasets/Bike+Sharing+Dataset) 營運提供。
// MAGIC 
// MAGIC **目標**: 預測出每個時段單車的需求量。

// COMMAND ----------

// MAGIC %md ## 把資料從 Azure Storage 讀取進來
// MAGIC 
// MAGIC 首先，我們把從 UCI 下載來的資料，上傳 _hour.csv_ 檔案到 Azure Storage Blob 中。

// COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://CONTAINER_NAME@STORAGE_ACCOUNT_NAME.blob.core.windows.net/",
  mountPoint = "/mnt/bikesharing",
  extraConfigs = Map("fs.azure.account.key.STORAGE_ACCOUNT_NAME.blob.core.windows.net" -> "STORAGE_ACCOUNT_KEY"))

// COMMAND ----------

// MAGIC %md
// MAGIC 將資料從 CSV 格式轉換成為 DataFrame 的資料結構，並確認資料都有讀進 DataFrame 之中，所以來計算它的筆數。

// COMMAND ----------

val dfRaw = spark.read.format("csv").option("header", "true").load("/mnt/bikesharing/hour.csv")
dfRaw.show()
println("資料筆數: " + dfRaw.count())

// COMMAND ----------

// MAGIC %md
// MAGIC 示範操作 DataFrame 的步驟，透過 `drop()` 方法把資料中不需要的欄位移除，最後顯示處理過後的資料樣貌。

// COMMAND ----------

val dfProcessed = dfRaw.drop("instant").drop("dteday").drop("casual").drop("registered")
dfProcessed.show()

// COMMAND ----------

// MAGIC %md
// MAGIC 同時觀察處理後的 Data Frame 的結構資訊。

// COMMAND ----------

dfProcessed.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC 示範如何將原本是 `String` 資料型態的資料轉換成 `double` 資料型態，並且印出結構資訊。

// COMMAND ----------

val dfTransformed = dfProcessed.select(dfProcessed.columns.map(c => dfProcessed.col(c).cast("double")): _*)
dfTransformed.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC 這裡使用 `randomSplit()` 方法將資料分為 training data 以及 testing data，下圖的寫法是將資料的 70% 作 training 用，而另外 30% 則用來驗證訓練完的模型。

// COMMAND ----------

val Array(dataTrain, dataTest) = dfTransformed.randomSplit(Array(0.7, 0.3))
println(s"接下來會用 ${dataTrain.count()} 筆資料做訓練，而用 ${dataTest.count()} 筆資料作為測試")

// COMMAND ----------

// MAGIC %md
// MAGIC 用圖表顯示 training data 中的 `hr` 以及 `cnt` 欄位，觀察每個時段的單車需求量。

// COMMAND ----------

display(dataTrain.select("hr", "cnt"))

// COMMAND ----------

// MAGIC %md
// MAGIC 將 `cnt` 欄位拿掉，其它欄位都當成是 feature columns。

// COMMAND ----------

val cols = dfTransformed.columns
val featureCols = cols.filter(!_.contains("cnt"))

// COMMAND ----------

// MAGIC %md
// MAGIC 將 feature columns 套入 Spark MLLib 中的向量資料結構中。這裡使用 Spark MLLib 中的 `GBTRegressor` 來作為訓練模型，而我們希望模型預測的就是 `cnt` 欄位的資料。設定驗證模型的參數以及驗證器。

// COMMAND ----------

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.evaluation.RegressionEvaluator

val va = new VectorAssembler()
            .setInputCols(featureCols)
            .setOutputCol("rawFeatures");

val vi = new VectorIndexer()
              .setInputCol("rawFeatures")
              .setOutputCol("features")
              .setMaxCategories(4)

val gbt = new GBTRegressor().setLabelCol("cnt")

// Define a grid of hyperparameters to test:
//  - maxDepth: max depth of each decision tree in the GBT ensemble
//  - maxIter: iterations, i.e., number of trees in each GBT ensemble
// In this example notebook, we keep these values small.  In practice, to get the highest accuracy, you would likely want to try deeper trees (10 or higher) and more trees in the ensemble (>100).
val paramGrid = new ParamGridBuilder()
                      .addGrid(gbt.maxDepth, Array(2, 5))
                      .addGrid(gbt.maxIter, Array(10, 100))
                      .build()
// We define an evaluation metric.  This tells CrossValidator how well we are doing by comparing the true labels with predictions.
val evaluator = new RegressionEvaluator()
                    .setMetricName("rmse")
                    .setLabelCol(gbt.getLabelCol)
                    .setPredictionCol(gbt.getPredictionCol)
// Declare the CrossValidator, which runs model tuning for us.
val cv = new CrossValidator()
              .setEstimator(gbt)
              .setEvaluator(evaluator)
              .setEstimatorParamMaps(paramGrid)

// COMMAND ----------

// MAGIC %md
// MAGIC 把訓練模型的 pipeline 定義好之後就能準備進行訓練了。

// COMMAND ----------

import org.apache.spark.ml.Pipeline

val pipeline = new Pipeline().setStages(Array(va, vi, cv))
val pipelineModel = pipeline.fit(dataTrain)

// COMMAND ----------

// MAGIC %md
// MAGIC 把測試資料 `dataTest` 放進訓練好的 `pipelineModel` 裡進行預測，之後再看結果。

// COMMAND ----------

val predictions = pipelineModel.transform(dataTest)
val tailCols = "prediction" +: featureCols
display(predictions.select("cnt",  tailCols:_*))

// COMMAND ----------

// MAGIC %md
// MAGIC 計算使用測試資料後的預測值與正確答案的 RMSE (Root-Mean-Square-Error) 數值，藉此來判斷訓練好的模型是否準確。

// COMMAND ----------

val rmse = evaluator.evaluate(predictions)
println(s"Root-Mean-Square-Error on our test set: ${rmse}")

// COMMAND ----------

// MAGIC %md
// MAGIC 比對訓練資料與預測結果的圖表。

// COMMAND ----------

display(dataTrain.select("hr", "cnt"))

// COMMAND ----------

display(predictions.select("hr", "prediction"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 把預測結果儲存到 SQL Data Warehouse 中
// MAGIC 
// MAGIC 提供可從 Azure Databricks 存取 Azure 儲存體帳戶的組態。

// COMMAND ----------

val blobStorage = "STORAGE_ACCOUNT_NAME.blob.core.windows.net"
val blobContainer = "CONTAINER_NAME"
val blobAccessKey =  "STORAGE_ACCOUNT_KEY"

// COMMAND ----------

// MAGIC %md
// MAGIC 指定在 Azure Databricks 與 Azure SQL Data Warehouse 之間移動資料所用的暫存資料夾。

// COMMAND ----------

 val tempDir = "wasbs://" + blobContainer + "@" + blobStorage +"/tempDirs"

// COMMAND ----------

// MAGIC %md
// MAGIC 執行下列程式碼片段，儲存組態中的 Azure Blob 儲存體存取金鑰。 這可確保您不必在 Notebook 中以純文字保留存取金鑰。

// COMMAND ----------

val acntInfo = "fs.azure.account.key."+ blobStorage
 sc.hadoopConfiguration.set(acntInfo, blobAccessKey)

// COMMAND ----------

// MAGIC %md
// MAGIC 提供用來連線至 Azure SQL 資料倉儲執行個體的值。 您必須已建立屬於必要條件的 SQL 資料倉儲。

// COMMAND ----------

//SQL Data Warehouse related settings
 val dwDatabase = "DATABASE_NAME"
 val dwServer = "DATABASE_SERVER_HOST" 
 val dwUser = "USERNAME"
 val dwPass = "PASSWORD"
 val dwJdbcPort =  "1433"
 val dwJdbcExtraOptions = "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
 val sqlDwUrl = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass + ";$dwJdbcExtraOptions"
 val sqlDwUrlSmall = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass

// COMMAND ----------

// MAGIC %md
// MAGIC 執行下列程式碼片段，將已轉換的資料框架 predictions 以資料表形式載入 SQL 資料倉儲中。 此程式碼片段會在 SQL 資料庫中建立名為 SampleTable 的資料表。 請注意，Azure SQL DW 需要主要金鑰。 您可以在 SQL Server Management Studio 中執行 "CREATE MASTER KEY;" 命令，以建立主要金鑰。

// COMMAND ----------

spark.conf.set(
   "spark.sql.parquet.writeLegacyFormat",
   "true")

predictions.drop("rawFeatures").drop("features").write
     .format("com.databricks.spark.sqldw")
     .option("url", sqlDwUrlSmall) 
     .option("dbtable", "Predictions")
     .option( "forward_spark_azure_storage_credentials","True")
     .option("tempdir", tempDir)
     .mode("overwrite")
     .save()
