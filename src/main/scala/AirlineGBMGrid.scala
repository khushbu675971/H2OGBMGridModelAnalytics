import java.io.File
import hex.splitframe.ShuffleSplitFrame
import hex.tree.gbm.GBMModel
import hex.{ModelMetrics, ModelMetricsSupervised}
import org.apache.spark.h2o._
import org.apache.spark.sql.SparkSession
import water.Key
import water.fvec.{Frame, H2OFrame}
import water.support.{SparkContextSupport, SparkSessionSupport}

object AirlineGBMGrid extends SparkContextSupport with SparkSessionSupport {

  def r2(model: GBMModel, fr: Frame) = ModelMetrics.getFromDKV(model, fr).asInstanceOf[ModelMetricsSupervised].r2()
//Function to create model
  def buildModel(df: H2OFrame)(implicit h2oContext: H2OContext) = {
    import h2oContext.implicits._
    val keys = Array[String]("AirlinesTrain.hex", "valid.hex").map(Key.make[Frame](_))
    val ratios = Array[Double](0.8, 0.1)
    val frs = ShuffleSplitFrame.shuffleSplitFrame(df, keys, ratios, 1234567689L)
    val train = frs(0)
    val valid = frs(1)

    import hex.tree.gbm.GBM
    import hex.tree.gbm.GBMModel.GBMParameters

    val gbmParams = new GBMParameters()
    gbmParams._train = train
    gbmParams._valid = valid
    gbmParams._response_column = 'IsDepDelayed
    gbmParams._ntrees = 200

    val gbm = new GBM(gbmParams)
    val gbmModel = gbm.trainModel.get

    gbmModel.score(train).remove()
    gbmModel.score(valid).remove()

    println(
      s"""
         |r2 on train: ${r2(gbmModel, train)}
         |r2 on valid:  ${r2(gbmModel, valid)}"""".stripMargin)
    train.delete()
    valid.delete()

    gbmModel
  }

  def reformatH2OFrame(h2oFrame: H2OFrame)(implicit h2oContext: H2OContext) = {
    import h2oContext._
    val tempDF = asDataFrame(h2oFrame)

    import org.apache.spark.sql.functions.{col, udf}
    val coder: String => Int = (arg: String) => {
      if (arg == "YES") 1 else 0
    }
    val stringToInt = udf(coder)
    val df = tempDF.withColumn("test", stringToInt(col("IsDepDelayed")))
      .drop("IsDepDelayed")
      .withColumnRenamed("test", "IsDepDelayed")
    h2oContext.asH2OFrame(df)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("GBM Model")
      .master("local[8]")
      .getOrCreate()

    // Create SQL support
    val sqlContext = spark.sqlContext

    // Start H2O services
    val sc = spark.sparkContext
    val h2oContext = H2OContext.getOrCreate(sc)
    import h2oContext._
    import h2oContext.implicits._

    val trainDataInput = "src/main/resources/AirlinesTrain.csv"
    val testDataInput = "src/main/resources/AirlinesTest.csv"
    val prediction_result = "src/main/resources/prediction_result"

    // Import prostate data into H2O
    val airlineTrainData = new H2OFrame(new File(trainDataInput))
    val airlineTestData = new H2OFrame(new File(testDataInput))

    // Reformat H2OFrame
    val dfTrainH2O = reformatH2OFrame(airlineTrainData)(h2oContext)
    val dfTestH2O = reformatH2OFrame(airlineTestData)(h2oContext)

    val gbmModel = buildModel(dfTrainH2O)(h2oContext)

    // Use model to estimate delay on training data
    val predGBMH2OFrame = gbmModel.score(dfTestH2O)('predict)
    val predictonResults = asRDD[DoubleHolder](predGBMH2OFrame).collect.map(_.result.getOrElse(Double.NaN))

    println("***** Top 10 prediction *****")
    predictonResults.take(10).map(println)
    //Save the prediction in one file
   // sc.parallelize(predictonResults).coalesce(1,true).saveAsTextFile(prediction_result)

    println("***** Grid Search *****")
    // Grid Search
    def let[A](in: A)(body: A => Unit) = {
      body(in)
      in
    }

    import _root_.hex.ScoreKeeper
    import _root_.hex.grid.GridSearch
    import water.Key
    import scala.collection.JavaConversions._
// define hyper parameters
    val gbmHyperSpace: java.util.Map[String, Array[Object]] = Map[String, Array[AnyRef]](
      "_max_depth" -> (1 to 9).map(Int.box).toArray,
      "_learn_rate" -> Array(0.1, 0.01, 0.001).map(Double.box),
      "_col_sample_rate" -> Array(0.3, 0.7, 1.0).map(Double.box),
      "_learn_rate_annealing" -> Array(0.5, 0.6, 0.7, 1.0).map(Double.box)
    )

    import _root_.hex.grid.HyperSpaceSearchCriteria.RandomDiscreteValueSearchCriteria

    // H2O GBM Reference
    import _root_.hex.tree.gbm.GBMModel.GBMParameters

    val gbmHyperSpaceCriteria = let(new RandomDiscreteValueSearchCriteria) { c =>
      c.set_stopping_metric(ScoreKeeper.StoppingMetric.RMSE)
      c.set_stopping_tolerance(0.01)
      c.set_stopping_rounds(3)
      c.set_max_runtime_secs(40 * 60 /* seconds */)
      c.set_max_models(3)
    }

    val gbmGrid = GridSearch.startGridSearch(Key.make("gbmGridModel"),
      gbmModel._parms,
      gbmHyperSpace,
      new GridSearch.SimpleParametersBuilderFactory[GBMParameters],
      gbmHyperSpaceCriteria).get()


    // Training Frame Info
    println(gbmGrid.getTrainingFrame)

    // Looking at grid models by Keys
    val mKeys = gbmGrid.getModelKeys()
    println(mKeys)
    gbmGrid.createSummaryTable(mKeys, "mse", true)
    gbmGrid.createSummaryTable(mKeys, "rmse", true)

    // Model Count
    println(gbmGrid.getModelCount)

    // All Models
    val ms = gbmGrid.getModels()
    println(ms.size)

    // All hyper parameters
    println(gbmGrid.getHyperNames)

    h2oContext.stop(stopSparkContext = true)
  }
}