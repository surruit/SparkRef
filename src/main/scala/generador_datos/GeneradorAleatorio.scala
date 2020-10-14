package generador_datos

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{SparkSession, functions}

object GeneradorAleatorio {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("GeneradorAleatorio")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.range(20000000000L)

    val df_datos = df
      .withColumn("id_categoria", func_randomNum(0, 1000000)())
      .withColumn("salario", func_randomNum(15000, 50000)())
      .withColumn("dimension1", func_randomEnum(Seq("A", "B", "C", "D"))())
      .withColumn("dimension2", func_randomEnumProb(
        Map(
          "Hombre" -> 0.9F,
          "Mujer" -> 0.05F,
          "Otro" -> 0.05F
        ))())

    df_datos.write.format("parquet").saveAsTable("default.empleados")
  }

  def func_randomNum(min: Int, max: Int): UserDefinedFunction ={

    val aleatorio: () => Int = () => {
      Math.floor(min + (Math.random()*(max-min))).toInt
    }

    functions.udf(aleatorio)
  }

  def func_randomEnum(seq: Seq[String]): UserDefinedFunction = {
    val min = 0
    val max = seq.size

    val aleatorio: () => String = () => {
      val randomIndex = Math.floor(min + (Math.random()*(max-min))).toInt

      seq(randomIndex)
    }

    functions.udf(aleatorio)
  }

  def func_randomEnumProb(values: Map[String, Float]): UserDefinedFunction = {

    //Validar probabilidad
    if (values.values.sum != 1) throw new Error("La suma total de las probabilidades no es 100%")

    //convierte los numeros de porcentaje a rangos de 0 a 1.
    var acum = 0F
    def generarRangos(mapa: (String, Float)): (String, (Float, Float)) ={
      val min = acum
      val max = acum + mapa._2

      acum = max

      (mapa._1, (min, max))
    }

    val rangos = values.map(generarRangos)

    val selectRandomVal: () => String = () => {
      val randomNum = Math.random()

      def filterFun(rangeElement: (String, (Float, Float))) = rangeElement._2._1 < randomNum && rangeElement._2._2 > randomNum

      rangos.filter(filterFun).head._1
    }

    functions.udf(selectRandomVal)
  }
}
