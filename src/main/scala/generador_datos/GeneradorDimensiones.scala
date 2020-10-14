package generador_datos

import org.apache.spark.sql.SparkSession

object GeneradorDimensiones {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("GeneradorDimensiones")
      .enableHiveSupport()
      .getOrCreate()


    val valores = Seq(
      (1, "A", "AKF155"),
      (2, "B", "BKF151"),
      (3, "C", "AKF155"),
      (4, "D", "AKF155")
    )

    val columnas = Seq(
      "id_dimension1", "valor_dimension1", "codigo_especial"
    )

    import spark.implicits._

    val df_dimension = valores.toDF(columnas:_*)

    df_dimension.repartition(1).write.format("parquet").saveAsTable("default.dimension1")
  }
}
