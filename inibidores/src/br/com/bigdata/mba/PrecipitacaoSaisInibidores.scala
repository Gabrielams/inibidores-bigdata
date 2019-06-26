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

object PrecipitacaoSaisInibidores {

def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: PrecipitacaoSaisInibidores diretorio")
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
    
    val filtroMaior1K = atividades.filter($"contagem" > "1000.00" && $"contagem" < "2000.00").withColumnRenamed("value", "contagem")
    filtroMaior1K.sort($"contagem")
    
/*    val cont = filtroMaior1K.groupBy("contagem")
    	.count
    	.sort($"contagem".desc)
    	.withColumnRenamed("count","contador")*/
    
    val filtroMaior2K = atividades.filter($"contagem" > "2000.00" && $"contagem" < "3000.00" ).withColumnRenamed("value", "contagem")
    filtroMaior2K.sort($"contagem")
    
    val filtroMaior3K = atividades.filter($"contagem" > "3000.00" && $"contagem" < "4000.00" ).withColumnRenamed("value", "contagem")
    filtroMaior3K.sort($"contagem")

    
    val filtroMaior4K = atividades.filter($"contagem" > "4000.00" && $"contagem" < "5000.00" ).withColumnRenamed("value", "contagem")
    filtroMaior4K.sort($"contagem")

    
    val filtroMaior5K = atividades.filter($"contagem" > "5000.00" && $"contagem" < "6000.00" ).withColumnRenamed("value", "contagem")  
    filtroMaior5K.sort($"contagem")

    val filtroMaior6K = atividades.filter($"contagem" > "6000.00" && $"contagem" < "7000.00" ).withColumnRenamed("value", "contagem")
    filtroMaior6K.sort($"contagem")  
    
    
    val filtroMaior7K = atividades.filter($"contagem" > "7000.00" && $"contagem" < "8000.00" ).withColumnRenamed("value", "contagem")
    filtroMaior7K.sort($"contagem")

    
    val filtroMaior8K = atividades.filter($"contagem" > "8000.00" && $"contagem" < "9000.00").withColumnRenamed("value", "contagem")
    filtroMaior8K.sort($"contagem")

    
    val filtroMaior10K = atividades.filter($"contagem" > "10000.00").withColumnRenamed("value", "contagem")
    filtroMaior10K.sort($"contagem")

    System.out.println("antes do WS")

    val query = filtroMaior1K.writeStream
      .outputMode(Append)
      //.outputMode(Update) // Não pode ser usado com orderBy()
      .format("csv")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .option("path", "/Bigdata/saida_arquivo/01")
      .option("checkpointLocation", "/Users/usuario/Arquivo-Marco/BigDataScience/chckptcsv/chckptcsv_01")
      .start()
      
    val query2 = filtroMaior2K.writeStream
      .outputMode(Append)
      //.outputMode(Update) // Não pode ser usado com orderBy()
      .format("csv")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .option("path", "/Bigdata/saida_arquivo/02")
      .option("checkpointLocation", "/Users/usuario/Arquivo-Marco/BigDataScience/chckptcsv/chckptcsv_02")
      .start()
      
    val query3 = filtroMaior3K.writeStream
      .outputMode(Append)
      //.outputMode(Update) // Não pode ser usado com orderBy()
      .format("csv")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .option("path", "/Bigdata/saida_arquivo/03")
      .option("checkpointLocation", "/Users/usuario/Arquivo-Marco/BigDataScience/chckptcsv/chckptcsv_03")
      .start()      

    val query4 = filtroMaior4K.writeStream
      .outputMode(Append)
      //.outputMode(Update) // Não pode ser usado com orderBy()
      .format("csv")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .option("path", "/Bigdata/saida_arquivo/04")
      .option("checkpointLocation", "/Users/usuario/Arquivo-Marco/BigDataScience/chckptcsv/chckptcsv_04")
      .start()
      
    val query5 = filtroMaior5K.writeStream
      .outputMode(Append)
      //.outputMode(Update) // Não pode ser usado com orderBy()
      .format("csv")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .option("path", "/Bigdata/saida_arquivo/05")
      .option("checkpointLocation", "/Users/usuario/Arquivo-Marco/BigDataScience/chckptcsv/chckptcsv_05")
      .start()
      
    val query6 = filtroMaior6K.writeStream
      .outputMode(Append)
      //.outputMode(Update) // Não pode ser usado com orderBy()
      .format("csv")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .option("path", "/Bigdata/saida_arquivo/06")
      .option("checkpointLocation", "/Users/usuario/Arquivo-Marco/BigDataScience/chckptcsv/chckptcsv_06")
      .start() 
      
    val query7 = filtroMaior7K.writeStream
      .outputMode(Append)
      //.outputMode(Update) // Não pode ser usado com orderBy()
      .format("csv")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .option("path", "/Bigdata/saida_arquivo/07")
      .option("checkpointLocation", "/Users/usuario/Arquivo-Marco/BigDataScience/chckptcsv/chckptcsv_07")
      .start()
      
    val query8 = filtroMaior8K.writeStream
      .outputMode(Append)
      //.outputMode(Update) // Não pode ser usado com orderBy()
      .format("csv")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .option("path", "/Bigdata/saida_arquivo/08")
      .option("checkpointLocation", "/Users/usuario/Arquivo-Marco/BigDataScience/chckptcsv/chckptcsv_08")
      .start()
      
    val query9 = filtroMaior10K.writeStream
      .outputMode(Append)
      //.outputMode(Update) // Não pode ser usado com orderBy()
      .format("csv")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .option("path", "/Bigdata/saida_arquivo/09")
      .option("checkpointLocation", "/Users/usuario/Arquivo-Marco/BigDataScience/chckptcsv/chckptcsv_09")
      .start()        
      
    System.out.println("depois do WS")

    while (scala.io.StdIn.readLine("X + [ENTER] para sair! ").trim.toUpperCase != "X") {
    }
    println("Encerrando...")
    query.stop()
    println("Fim!")

    query.awaitTermination()
  }
}