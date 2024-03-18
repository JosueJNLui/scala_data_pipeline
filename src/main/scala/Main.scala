package com.josuejnlui.datapipeline

import org.apache.hadoop.shaded.org.eclipse.jetty.util.ajax.JSON
import org.apache.spark.sql.{DataFrame, SparkSession}

class SparkData() {

  private def fileFormat(path: String): String = {

    val indexLastDot: Int = path.lastIndexOf('.')
    val format: String = path.substring(indexLastDot+1)
    format
  }

  def sparkReadFile(spark: org.apache.spark.sql.SparkSession, path: String): org.apache.spark.sql.DataFrame = {

    val format: String = fileFormat(path)
    val sparkDataFrame: DataFrame = if (format == "json") {
      spark.read
        .options(Map(
          "multiLine" -> "true",
        ))
        .json(path)
    } else if (format == "csv") {
      spark.read
        .options(Map(
          "header" -> "true",
          "multiLine" -> "true"
        ))
        .csv(path)
    } else {
      throw new IllegalArgumentException("Unsupported format: " + format)
    }
    sparkDataFrame
  }

  def renameColumns(sparkDataFrame: org.apache.spark.sql.DataFrame, keyMapping: Map[String, String]) : org.apache.spark.sql.DataFrame = {
    val renamedSparkDataFrame: DataFrame = keyMapping.foldLeft(sparkDataFrame) {
      case (tempDF, (oldName, newName)) => tempDF.withColumnRenamed(oldName, newName)
    }
    renamedSparkDataFrame
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("spark-data-pipeline")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val sparkData = new SparkData()
    var dataFrameA = sparkData.sparkReadFile(spark, "dataRaw/dados_empresaA.json")
    var dataFrameB = sparkData.sparkReadFile(spark, "dataRaw/dados_empresaB.csv")

    val keyMapping: Map[String, String] = Map(
      "Nome do Item" -> "Nome do Produto",
      "Classificação do Produto" -> "Categoria do Produto",
      "Valor em Reais (R$)" -> "Preço do Produto (R$)",
      "Quantidade em Estoque" -> "Quantidade em Estoque",
      "Nome da Loja" -> "Filial",
      "Data da Venda" -> "Data da Venda"
    )

    dataFrameB = sparkData.renameColumns(dataFrameB, keyMapping)

  }
}