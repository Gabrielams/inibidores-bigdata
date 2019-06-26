package br.com.bigdata.mba

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.streaming.OutputMode._
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._
import java.sql.Timestamp

class PrecipitacaoSaisInibidores {

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: PrecipitacaoSaisInibidores")
      System.exit(1)
    }


    val diretorio: String = args(0)

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("PrecipitacaoSaisInibidores")
      .getOrCreate()

    import spark.implicits._

    val esquema = StructType(
      StructField("data", StringType, true) ::
        StructField("time", StringType, true) ::
        StructField("segundo", StringType, true) ::
        StructField("minuto", StringType, true) ::
        StructField("contagem", DoubleType, true) ::
        StructField("media", StringType, true) :: 
        StructField("inibidor", StringType, true) :: Nil)

    val leituras = spark.readStream
      .schema(esquema)
      .csv(diretorio)

    val atividades = leituras
      .select($"data", $"time", $"contagem", $"media", $"inibidor")
      .as[RealTime]

    val filtroQtd = atividades.filter($"contagem" < "1000.00").withColumnRenamed("value", "contagem")

    System.out.println("antes do WS")

    val query = filtroQtd.writeStream
      .outputMode(Append)
      //.outputMode(Update) // Não pode ser usado com orderBy()
      .format("csv")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .option("path", "/Bigdata/saida_arquivo/")
      .option("checkpointLocation", "/Users/usuario/Arquivo-Marco/BigDataScience/chckptcsv_Ex00_PrecipitacaoDeSais")
      .start()

    System.out.println("depois do WS")

    /*    val query = filtroQtd.writeStream
      .outputMode(Complete)
      .format("console")
      .start
      */
    while (scala.io.StdIn.readLine("X + [ENTER] para sair! ").trim.toUpperCase != "X") {
    }
    println("Encerrando...")
    query.stop()
    println("Fim!")

    query.awaitTermination()
  }
}