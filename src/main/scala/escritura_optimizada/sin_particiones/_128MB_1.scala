package escritura_optimizada.sin_particiones

import generador_datos.GeneradorAleatorio.{func_randomEnum, func_randomEnumProb, func_randomNum}
import WriterUtils._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object _128MB_1 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("SinParticiones_128MB")
      .enableHiveSupport()
      .getOrCreate()

    val df_datos = getDataframe(spark)

    val sparkStaging = getSparkStagingDir(spark).get

    val sizePerRow = calcSizePerRow(spark, df_datos, sparkStaging)

    val partitions = calcTargetPartitions(df_datos, sizePerRow, 128)

    df_datos
      .repartition(partitions)
      .write
      .format("parquet")
      .option("compression","snappy")
      .mode(SaveMode.Overwrite)
      .saveAsTable("default.empleados")
  }

  //Genera un dataframe de datos aleatorios
  def getDataframe(spark: SparkSession): DataFrame = {
    val df_1B = spark.range(0, 1000000000L, 1, 200)

    val df_datos = df_1B
      .withColumn("id_categoria", func_randomNum(0, 1000000)())
      .withColumn("salario", func_randomNum(15000, 50000)())
      .withColumn("dimension1", func_randomEnum(Seq("A", "B", "C", "D"))())
      .withColumn("dimension2", func_randomEnumProb(
        Map(
          "Hombre" -> 0.8F,
          "Mujer" -> 0.15F,
          "Otro" -> 0.05F
        ))())

    df_datos
  }


}
