package escritura_optimizada

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import generador_datos.GeneradorAleatorio._

object SinParticiones128MB {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("SinParticiones128MB")
      .enableHiveSupport()
      .getOrCreate()

    val df_datos = getDataframe(spark)

    val sparkStaging = getSparkStagingDir(spark).get

    val sizePerRow1 = calcSizePerRow(spark, df_datos, sparkStaging)

    val sizePerRow2 = calcSizePerRow2(spark, df_datos, sparkStaging)

    val partitions = calcTargetPartitions(df_datos, sizePerRow1, 128)

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

  //calcula el path a la ruta de hdfs "/user/${user}/.sparkStaging/application_1602582108394_0023"
  def getSparkStagingDir(spark: SparkSession): Option[String] ={
    val user = spark.sparkContext.sparkUser
    val applicationId = spark.sparkContext.applicationId

    val hdfsDir = new Path("/user/" + user + "/"  + ".sparkStaging/" + applicationId)

    val hadoopFS = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    if (hadoopFS.exists(hdfsDir)) Some(hdfsDir.toString) else None
  }

  //Calcula el tamaño por registro, utiliza un muestreo no aleatorio de 1000 registros (muy rapido, muy impreciso)
  def calcSizePerRow(spark: SparkSession, dataFrame: DataFrame, tempFolder: String): Double ={
    println("Partition count: " + dataFrame.rdd.partitions.length)
    println("StagingDir: " + tempFolder)

    //Escribe a disco una pequeña muestra del dataframe para calcular su ocupacion en disco
    val df_reduced = dataFrame
      .limit(1000)
      .repartition(1)

    require(df_reduced.count() == 1000)

    df_reduced
      .write
      .option("compression", "snappy")
      .parquet(tempFolder + "/EstimadorTamanio")

    val hadoopFS = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val folderPath = new Path(tempFolder + "/EstimadorTamanio")

    val bytes1000 = hadoopFS.getContentSummary(folderPath).getLength.toDouble

    hadoopFS.delete(folderPath, true)

    println("Tamaño calculado para 1000 registros: " + bytes1000 + " Bytes")
    println("Tamaño por registro: " + bytes1000/1000 + " Bytes")

    bytes1000/1000
  }

  //Calcula el tamaño por registro, utiliza un muestreo no aleatorio de 10000 registros (rapido, impreciso)
  def calcSizePerRow2(spark: SparkSession, dataFrame: DataFrame, tempFolder: String): Double ={
    println("Partition count: " + dataFrame.rdd.partitions.length)
    println("StagingDir: " + tempFolder)

    //Escribe a disco una pequeña muestra del dataframe para calcular su ocupacion en disco
    val df_reduced = dataFrame
      .limit(10000)
      .repartition(1)

    require(df_reduced.count() == 10000)

    df_reduced
      .write
      .option("compression", "snappy")
      .parquet(tempFolder + "/EstimadorTamanio")

    val hadoopFS = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val folderPath = new Path(tempFolder + "/EstimadorTamanio")

    val bytes10000 = hadoopFS.getContentSummary(folderPath).getLength.toDouble

    hadoopFS.delete(folderPath, true)

    println("Tamaño calculado para 10000 registros: " + bytes10000 + " Bytes")
    println("Tamaño por registro: " + bytes10000/10000 + " Bytes")

    bytes10000/10000
  }

  //Calcula cuantas particiones son necesarias para el tamaño de fichero objetivo
  def calcTargetPartitions(df: DataFrame, sizePerRow: Double, targetSizePerFile: Double): Int ={
    val totalCount = df.count()

    val totalSize = totalCount*sizePerRow/1024/1024 //MB

    println("Tamaño total estimado: " + totalSize + " MB")

    val partitions = Math.ceil(totalSize/targetSizePerFile).toInt

    println("Particiones estimadas: " + partitions)

    partitions
  }
}
