package escritura_optimizada.sin_particiones

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}

object WriterUtils {
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

  //Calcula el tamaño por registro, utiliza un muestreo no aleatorio de 250000 registros (lento, preciso)
  def calcSizePerRow3(spark: SparkSession, dataFrame: DataFrame, tempFolder: String): Double ={
    println("Partition count: " + dataFrame.rdd.partitions.length)
    println("StagingDir: " + tempFolder)

    val totalCount = dataFrame.count()

    val sampleFraction = (250000D/totalCount)* 1.5

    //Escribe a disco una pequeña muestra del dataframe para calcular su ocupacion en disco
    val df_reduced = dataFrame
      .sample(false, sampleFraction)
      .limit(250000)
      .repartition(1)

    require(df_reduced.count() == 250000)

    df_reduced
      .write
      .option("compression", "snappy")
      .parquet(tempFolder + "/EstimadorTamanio")

    val hadoopFS = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val folderPath = new Path(tempFolder + "/EstimadorTamanio")

    val bytes250000 = hadoopFS.getContentSummary(folderPath).getLength.toDouble

    hadoopFS.delete(folderPath, true)

    println("Tamaño calculado para 250000 registros: " + bytes250000 + " Bytes")
    println("Tamaño por registro: " + bytes250000/250000 + " Bytes")

    bytes250000/250000
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