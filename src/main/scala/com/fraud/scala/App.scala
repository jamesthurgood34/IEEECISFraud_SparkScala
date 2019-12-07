package com.fraud.scala

import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => F, types => T}
import org.apache.spark.sql.{DataFrame}

object fraud extends App {

  implicit class DataFrameFunctions(df: DataFrame) {

    /** Convert [[org.apache.spark.sql.DataFrame]] from wide to long format.
     *
     *  melt is (kind of) the inverse of pivot
     *  melt is currently (02/2017) not implemented in spark
     *
     *  @see reshape packe in R (https://cran.r-project.org/web/packages/reshape/index.html)
     *  @see this is a scala adaptation of http://stackoverflow.com/questions/41670103/pandas-melt-function-in-apache-spark
     *
     *  @todo method overloading for simple calling
     *
     *  @param id_vars the columns to preserve
     *  @param value_vars the columns to melt
     *  @param var_name the name for the column holding the melted columns names
     *  @param value_name the name for the column holding the values of the melted columns
     *
     */

    def melt(
              id_vars: Seq[String], value_vars: Seq[String],
              var_name: String = "variable", value_name: String = "value") : DataFrame = {

      // Create array<struct<variable: str, value: ...>>
      val _vars_and_vals = F.array((for (c <- value_vars) yield { F.struct(F.lit(c).alias(var_name), F.col(c).alias(value_name)) }): _*)

      // Add to the DataFrame and explode
      val _tmp = df.withColumn("_vars_and_vals", F.explode(_vars_and_vals))

      val cols = id_vars.map(F.col _) ++ { for (x <- List(var_name, value_name)) yield { F.col("_vars_and_vals")(x).alias(x) }}

      return _tmp.select(cols: _*)

    }

    def howManyNulls(): DataFrame ={
      df.select(df.columns.map(c => F.sum(F.when(F.col(c).isNull,  value = 1).otherwise( value = 0)).alias(c)):_*)
    }

    def percentNulls(): DataFrame = {

      val nRows = df.count().toDouble
      val _tmp = df.howManyNulls()

      return _tmp.select(_tmp.columns.map(c => (F.col(c)/nRows).alias(c)):_*)
    }

    def removeNullRows(): Unit = {

      val isNull = df.select(df.columns.map(c => (F.when(F.col(c).isNull,  value = 1).otherwise( value = 0)).alias(c)):_*)
      isNull.show
      val toAdd = isNull
                    .columns
                    .dropWhile(x => x != "TransactionID")
                    .map(x => F.col(x))

      //isNull
      //  .withColumn("noNulls",toAdd.reduce(_ + _))
      //  .where(F.col("noNulls") == 0 )

    }

  }


    val spark = SparkSession
      .builder()
      .master(master = "local[6]")
      .appName(name = "IEE_CIS_Fraud")
      .getOrCreate()

    import spark.implicits._

    val transactionSchema = T.StructType(Array(

      T.StructField("TransactionID", T.IntegerType, true),
      T.StructField("isFraud", T.IntegerType, true),
      T.StructField("TransactionDT", T.IntegerType, true),
      T.StructField("TransactionAmt", T.DoubleType, true),
      T.StructField("ProductCD", T.StringType, true),
      T.StructField("card1", T.IntegerType, true),
      T.StructField("card2", T.DoubleType, true),
      T.StructField("card3", T.DoubleType, true),
      T.StructField("card4", T.StringType, true),
      T.StructField("card5", T.DoubleType, true),
      T.StructField("card6", T.StringType, true),
      T.StructField("addr1", T.DoubleType, true),
      T.StructField("addr2", T.DoubleType, true),
      T.StructField("dist1", T.DoubleType, true),
      T.StructField("dist2", T.DoubleType, true),
      T.StructField("P_emaildomain", T.StringType, true),
      T.StructField("R_emaildomain", T.StringType, true),
      T.StructField("C1", T.DoubleType, true),
      T.StructField("C2", T.DoubleType, true),
      T.StructField("C3", T.DoubleType, true),
      T.StructField("C4", T.DoubleType, true),
      T.StructField("C5", T.DoubleType, true),
      T.StructField("C6", T.DoubleType, true),
      T.StructField("C7", T.DoubleType, true),
      T.StructField("C8", T.DoubleType, true),
      T.StructField("C9", T.DoubleType, true),
      T.StructField("C10", T.DoubleType, true),
      T.StructField("C11", T.DoubleType, true),
      T.StructField("C12", T.DoubleType, true),
      T.StructField("C13", T.DoubleType, true),
      T.StructField("C14", T.DoubleType, true),
      T.StructField("D1", T.DoubleType, true),
      T.StructField("D2", T.DoubleType, true),
      T.StructField("D3", T.DoubleType, true),
      T.StructField("D4", T.DoubleType, true),
      T.StructField("D5", T.DoubleType, true),
      T.StructField("D6", T.DoubleType, true),
      T.StructField("D7", T.DoubleType, true),
      T.StructField("D8", T.DoubleType, true),
      T.StructField("D9", T.DoubleType, true),
      T.StructField("D10", T.DoubleType, true),
      T.StructField("D11", T.DoubleType, true),
      T.StructField("D12", T.DoubleType, true),
      T.StructField("D13", T.DoubleType, true),
      T.StructField("D14", T.DoubleType, true),
      T.StructField("D15", T.DoubleType, true),
      T.StructField("M1", T.StringType, true),
      T.StructField("M2", T.StringType, true),
      T.StructField("M3", T.StringType, true),
      T.StructField("M4", T.StringType, true),
      T.StructField("M5", T.StringType, true),
      T.StructField("M6", T.StringType, true),
      T.StructField("M7", T.StringType, true),
      T.StructField("M8", T.StringType, true),
      T.StructField("M9", T.StringType, true),
      T.StructField("V1", T.DoubleType, true),
      T.StructField("V2", T.DoubleType, true),
      T.StructField("V3", T.DoubleType, true),
      T.StructField("V4", T.DoubleType, true),
      T.StructField("V5", T.DoubleType, true),
      T.StructField("V6", T.DoubleType, true),
      T.StructField("V7", T.DoubleType, true),
      T.StructField("V8", T.DoubleType, true),
      T.StructField("V9", T.DoubleType, true),
      T.StructField("V10", T.DoubleType, true),
      T.StructField("V11", T.DoubleType, true),
      T.StructField("V12", T.DoubleType, true),
      T.StructField("V13", T.DoubleType, true),
      T.StructField("V14", T.DoubleType, true),
      T.StructField("V15", T.DoubleType, true),
      T.StructField("V16", T.DoubleType, true),
      T.StructField("V17", T.DoubleType, true),
      T.StructField("V18", T.DoubleType, true),
      T.StructField("V19", T.DoubleType, true),
      T.StructField("V20", T.DoubleType, true),
      T.StructField("V21", T.DoubleType, true),
      T.StructField("V22", T.DoubleType, true),
      T.StructField("V23", T.DoubleType, true),
      T.StructField("V24", T.DoubleType, true),
      T.StructField("V25", T.DoubleType, true),
      T.StructField("V26", T.DoubleType, true),
      T.StructField("V27", T.DoubleType, true),
      T.StructField("V28", T.DoubleType, true),
      T.StructField("V29", T.DoubleType, true),
      T.StructField("V30", T.DoubleType, true),
      T.StructField("V31", T.DoubleType, true),
      T.StructField("V32", T.DoubleType, true),
      T.StructField("V33", T.DoubleType, true),
      T.StructField("V34", T.DoubleType, true),
      T.StructField("V35", T.DoubleType, true),
      T.StructField("V36", T.DoubleType, true),
      T.StructField("V37", T.DoubleType, true),
      T.StructField("V38", T.DoubleType, true),
      T.StructField("V39", T.DoubleType, true),
      T.StructField("V40", T.DoubleType, true),
      T.StructField("V41", T.DoubleType, true),
      T.StructField("V42", T.DoubleType, true),
      T.StructField("V43", T.DoubleType, true),
      T.StructField("V44", T.DoubleType, true),
      T.StructField("V45", T.DoubleType, true),
      T.StructField("V46", T.DoubleType, true),
      T.StructField("V47", T.DoubleType, true),
      T.StructField("V48", T.DoubleType, true),
      T.StructField("V49", T.DoubleType, true),
      T.StructField("V50", T.DoubleType, true),
      T.StructField("V51", T.DoubleType, true),
      T.StructField("V52", T.DoubleType, true),
      T.StructField("V53", T.DoubleType, true),
      T.StructField("V54", T.DoubleType, true),
      T.StructField("V55", T.DoubleType, true),
      T.StructField("V56", T.DoubleType, true),
      T.StructField("V57", T.DoubleType, true),
      T.StructField("V58", T.DoubleType, true),
      T.StructField("V59", T.DoubleType, true),
      T.StructField("V60", T.DoubleType, true),
      T.StructField("V61", T.DoubleType, true),
      T.StructField("V62", T.DoubleType, true),
      T.StructField("V63", T.DoubleType, true),
      T.StructField("V64", T.DoubleType, true),
      T.StructField("V65", T.DoubleType, true),
      T.StructField("V66", T.DoubleType, true),
      T.StructField("V67", T.DoubleType, true),
      T.StructField("V68", T.DoubleType, true),
      T.StructField("V69", T.DoubleType, true),
      T.StructField("V70", T.DoubleType, true),
      T.StructField("V71", T.DoubleType, true),
      T.StructField("V72", T.DoubleType, true),
      T.StructField("V73", T.DoubleType, true),
      T.StructField("V74", T.DoubleType, true),
      T.StructField("V75", T.DoubleType, true),
      T.StructField("V76", T.DoubleType, true),
      T.StructField("V77", T.DoubleType, true),
      T.StructField("V78", T.DoubleType, true),
      T.StructField("V79", T.DoubleType, true),
      T.StructField("V80", T.DoubleType, true),
      T.StructField("V81", T.DoubleType, true),
      T.StructField("V82", T.DoubleType, true),
      T.StructField("V83", T.DoubleType, true),
      T.StructField("V84", T.DoubleType, true),
      T.StructField("V85", T.DoubleType, true),
      T.StructField("V86", T.DoubleType, true),
      T.StructField("V87", T.DoubleType, true),
      T.StructField("V88", T.DoubleType, true),
      T.StructField("V89", T.DoubleType, true),
      T.StructField("V90", T.DoubleType, true),
      T.StructField("V91", T.DoubleType, true),
      T.StructField("V92", T.DoubleType, true),
      T.StructField("V93", T.DoubleType, true),
      T.StructField("V94", T.DoubleType, true),
      T.StructField("V95", T.DoubleType, true),
      T.StructField("V96", T.DoubleType, true),
      T.StructField("V97", T.DoubleType, true),
      T.StructField("V98", T.DoubleType, true),
      T.StructField("V99", T.DoubleType, true),
      T.StructField("V100", T.DoubleType, true),
      T.StructField("V101", T.DoubleType, true),
      T.StructField("V102", T.DoubleType, true),
      T.StructField("V103", T.DoubleType, true),
      T.StructField("V104", T.DoubleType, true),
      T.StructField("V105", T.DoubleType, true),
      T.StructField("V106", T.DoubleType, true),
      T.StructField("V107", T.DoubleType, true),
      T.StructField("V108", T.DoubleType, true),
      T.StructField("V109", T.DoubleType, true),
      T.StructField("V110", T.DoubleType, true),
      T.StructField("V111", T.DoubleType, true),
      T.StructField("V112", T.DoubleType, true),
      T.StructField("V113", T.DoubleType, true),
      T.StructField("V114", T.DoubleType, true),
      T.StructField("V115", T.DoubleType, true),
      T.StructField("V116", T.DoubleType, true),
      T.StructField("V117", T.DoubleType, true),
      T.StructField("V118", T.DoubleType, true),
      T.StructField("V119", T.DoubleType, true),
      T.StructField("V120", T.DoubleType, true),
      T.StructField("V121", T.DoubleType, true),
      T.StructField("V122", T.DoubleType, true),
      T.StructField("V123", T.DoubleType, true),
      T.StructField("V124", T.DoubleType, true),
      T.StructField("V125", T.DoubleType, true),
      T.StructField("V126", T.DoubleType, true),
      T.StructField("V127", T.DoubleType, true),
      T.StructField("V128", T.DoubleType, true),
      T.StructField("V129", T.DoubleType, true),
      T.StructField("V130", T.DoubleType, true),
      T.StructField("V131", T.DoubleType, true),
      T.StructField("V132", T.DoubleType, true),
      T.StructField("V133", T.DoubleType, true),
      T.StructField("V134", T.DoubleType, true),
      T.StructField("V135", T.DoubleType, true),
      T.StructField("V136", T.DoubleType, true),
      T.StructField("V137", T.DoubleType, true),
      T.StructField("V138", T.DoubleType, true),
      T.StructField("V139", T.DoubleType, true),
      T.StructField("V140", T.DoubleType, true),
      T.StructField("V141", T.DoubleType, true),
      T.StructField("V142", T.DoubleType, true),
      T.StructField("V143", T.DoubleType, true),
      T.StructField("V144", T.DoubleType, true),
      T.StructField("V145", T.DoubleType, true),
      T.StructField("V146", T.DoubleType, true),
      T.StructField("V147", T.DoubleType, true),
      T.StructField("V148", T.DoubleType, true),
      T.StructField("V149", T.DoubleType, true),
      T.StructField("V150", T.DoubleType, true),
      T.StructField("V151", T.DoubleType, true),
      T.StructField("V152", T.DoubleType, true),
      T.StructField("V153", T.DoubleType, true),
      T.StructField("V154", T.DoubleType, true),
      T.StructField("V155", T.DoubleType, true),
      T.StructField("V156", T.DoubleType, true),
      T.StructField("V157", T.DoubleType, true),
      T.StructField("V158", T.DoubleType, true),
      T.StructField("V159", T.DoubleType, true),
      T.StructField("V160", T.DoubleType, true),
      T.StructField("V161", T.DoubleType, true),
      T.StructField("V162", T.DoubleType, true),
      T.StructField("V163", T.DoubleType, true),
      T.StructField("V164", T.DoubleType, true),
      T.StructField("V165", T.DoubleType, true),
      T.StructField("V166", T.DoubleType, true),
      T.StructField("V167", T.DoubleType, true),
      T.StructField("V168", T.DoubleType, true),
      T.StructField("V169", T.DoubleType, true),
      T.StructField("V170", T.DoubleType, true),
      T.StructField("V171", T.DoubleType, true),
      T.StructField("V172", T.DoubleType, true),
      T.StructField("V173", T.DoubleType, true),
      T.StructField("V174", T.DoubleType, true),
      T.StructField("V175", T.DoubleType, true),
      T.StructField("V176", T.DoubleType, true),
      T.StructField("V177", T.DoubleType, true),
      T.StructField("V178", T.DoubleType, true),
      T.StructField("V179", T.DoubleType, true),
      T.StructField("V180", T.DoubleType, true),
      T.StructField("V181", T.DoubleType, true),
      T.StructField("V182", T.DoubleType, true),
      T.StructField("V183", T.DoubleType, true),
      T.StructField("V184", T.DoubleType, true),
      T.StructField("V185", T.DoubleType, true),
      T.StructField("V186", T.DoubleType, true),
      T.StructField("V187", T.DoubleType, true),
      T.StructField("V188", T.DoubleType, true),
      T.StructField("V189", T.DoubleType, true),
      T.StructField("V190", T.DoubleType, true),
      T.StructField("V191", T.DoubleType, true),
      T.StructField("V192", T.DoubleType, true),
      T.StructField("V193", T.DoubleType, true),
      T.StructField("V194", T.DoubleType, true),
      T.StructField("V195", T.DoubleType, true),
      T.StructField("V196", T.DoubleType, true),
      T.StructField("V197", T.DoubleType, true),
      T.StructField("V198", T.DoubleType, true),
      T.StructField("V199", T.DoubleType, true),
      T.StructField("V200", T.DoubleType, true),
      T.StructField("V201", T.DoubleType, true),
      T.StructField("V202", T.DoubleType, true),
      T.StructField("V203", T.DoubleType, true),
      T.StructField("V204", T.DoubleType, true),
      T.StructField("V205", T.DoubleType, true),
      T.StructField("V206", T.DoubleType, true),
      T.StructField("V207", T.DoubleType, true),
      T.StructField("V208", T.DoubleType, true),
      T.StructField("V209", T.DoubleType, true),
      T.StructField("V210", T.DoubleType, true),
      T.StructField("V211", T.DoubleType, true),
      T.StructField("V212", T.DoubleType, true),
      T.StructField("V213", T.DoubleType, true),
      T.StructField("V214", T.DoubleType, true),
      T.StructField("V215", T.DoubleType, true),
      T.StructField("V216", T.DoubleType, true),
      T.StructField("V217", T.DoubleType, true),
      T.StructField("V218", T.DoubleType, true),
      T.StructField("V219", T.DoubleType, true),
      T.StructField("V220", T.DoubleType, true),
      T.StructField("V221", T.DoubleType, true),
      T.StructField("V222", T.DoubleType, true),
      T.StructField("V223", T.DoubleType, true),
      T.StructField("V224", T.DoubleType, true),
      T.StructField("V225", T.DoubleType, true),
      T.StructField("V226", T.DoubleType, true),
      T.StructField("V227", T.DoubleType, true),
      T.StructField("V228", T.DoubleType, true),
      T.StructField("V229", T.DoubleType, true),
      T.StructField("V230", T.DoubleType, true),
      T.StructField("V231", T.DoubleType, true),
      T.StructField("V232", T.DoubleType, true),
      T.StructField("V233", T.DoubleType, true),
      T.StructField("V234", T.DoubleType, true),
      T.StructField("V235", T.DoubleType, true),
      T.StructField("V236", T.DoubleType, true),
      T.StructField("V237", T.DoubleType, true),
      T.StructField("V238", T.DoubleType, true),
      T.StructField("V239", T.DoubleType, true),
      T.StructField("V240", T.DoubleType, true),
      T.StructField("V241", T.DoubleType, true),
      T.StructField("V242", T.DoubleType, true),
      T.StructField("V243", T.DoubleType, true),
      T.StructField("V244", T.DoubleType, true),
      T.StructField("V245", T.DoubleType, true),
      T.StructField("V246", T.DoubleType, true),
      T.StructField("V247", T.DoubleType, true),
      T.StructField("V248", T.DoubleType, true),
      T.StructField("V249", T.DoubleType, true),
      T.StructField("V250", T.DoubleType, true),
      T.StructField("V251", T.DoubleType, true),
      T.StructField("V252", T.DoubleType, true),
      T.StructField("V253", T.DoubleType, true),
      T.StructField("V254", T.DoubleType, true),
      T.StructField("V255", T.DoubleType, true),
      T.StructField("V256", T.DoubleType, true),
      T.StructField("V257", T.DoubleType, true),
      T.StructField("V258", T.DoubleType, true),
      T.StructField("V259", T.DoubleType, true),
      T.StructField("V260", T.DoubleType, true),
      T.StructField("V261", T.DoubleType, true),
      T.StructField("V262", T.DoubleType, true),
      T.StructField("V263", T.DoubleType, true),
      T.StructField("V264", T.DoubleType, true),
      T.StructField("V265", T.DoubleType, true),
      T.StructField("V266", T.DoubleType, true),
      T.StructField("V267", T.DoubleType, true),
      T.StructField("V268", T.DoubleType, true),
      T.StructField("V269", T.DoubleType, true),
      T.StructField("V270", T.DoubleType, true),
      T.StructField("V271", T.DoubleType, true),
      T.StructField("V272", T.DoubleType, true),
      T.StructField("V273", T.DoubleType, true),
      T.StructField("V274", T.DoubleType, true),
      T.StructField("V275", T.DoubleType, true),
      T.StructField("V276", T.DoubleType, true),
      T.StructField("V277", T.DoubleType, true),
      T.StructField("V278", T.DoubleType, true),
      T.StructField("V279", T.DoubleType, true),
      T.StructField("V280", T.DoubleType, true),
      T.StructField("V281", T.DoubleType, true),
      T.StructField("V282", T.DoubleType, true),
      T.StructField("V283", T.DoubleType, true),
      T.StructField("V284", T.DoubleType, true),
      T.StructField("V285", T.DoubleType, true),
      T.StructField("V286", T.DoubleType, true),
      T.StructField("V287", T.DoubleType, true),
      T.StructField("V288", T.DoubleType, true),
      T.StructField("V289", T.DoubleType, true),
      T.StructField("V290", T.DoubleType, true),
      T.StructField("V291", T.DoubleType, true),
      T.StructField("V292", T.DoubleType, true),
      T.StructField("V293", T.DoubleType, true),
      T.StructField("V294", T.DoubleType, true),
      T.StructField("V295", T.DoubleType, true),
      T.StructField("V296", T.DoubleType, true),
      T.StructField("V297", T.DoubleType, true),
      T.StructField("V298", T.DoubleType, true),
      T.StructField("V299", T.DoubleType, true),
      T.StructField("V300", T.DoubleType, true),
      T.StructField("V301", T.DoubleType, true),
      T.StructField("V302", T.DoubleType, true),
      T.StructField("V303", T.DoubleType, true),
      T.StructField("V304", T.DoubleType, true),
      T.StructField("V305", T.DoubleType, true),
      T.StructField("V306", T.DoubleType, true),
      T.StructField("V307", T.DoubleType, true),
      T.StructField("V308", T.DoubleType, true),
      T.StructField("V309", T.DoubleType, true),
      T.StructField("V310", T.DoubleType, true),
      T.StructField("V311", T.DoubleType, true),
      T.StructField("V312", T.DoubleType, true),
      T.StructField("V313", T.DoubleType, true),
      T.StructField("V314", T.DoubleType, true),
      T.StructField("V315", T.DoubleType, true),
      T.StructField("V316", T.DoubleType, true),
      T.StructField("V317", T.DoubleType, true),
      T.StructField("V318", T.DoubleType, true),
      T.StructField("V319", T.DoubleType, true),
      T.StructField("V320", T.DoubleType, true),
      T.StructField("V321", T.DoubleType, true),
      T.StructField("V322", T.DoubleType, true),
      T.StructField("V323", T.DoubleType, true),
      T.StructField("V324", T.DoubleType, true),
      T.StructField("V325", T.DoubleType, true),
      T.StructField("V326", T.DoubleType, true),
      T.StructField("V327", T.DoubleType, true),
      T.StructField("V328", T.DoubleType, true),
      T.StructField("V329", T.DoubleType, true),
      T.StructField("V330", T.DoubleType, true),
      T.StructField("V331", T.DoubleType, true),
      T.StructField("V332", T.DoubleType, true),
      T.StructField("V333", T.DoubleType, true),
      T.StructField("V334", T.DoubleType, true),
      T.StructField("V335", T.DoubleType, true),
      T.StructField("V336", T.DoubleType, true),
      T.StructField("V337", T.DoubleType, true),
      T.StructField("V338", T.DoubleType, true),
      T.StructField("V339", T.DoubleType, true)
    ))

    val trans = spark.read.format(source = "csv")
      .schema(transactionSchema)
      .option("sep", ",")
      .option("header", "true")
      .load(path = "data/train_transaction.csv")
      .coalesce(numPartitions = 6)

    trans.persist(StorageLevel.MEMORY_ONLY)

    //val id = spark.read.format( source = "csv")
    //  .option("sep", ",")
    //  .option("inferSchema", "true")
    //  .option("header", "true")
    //  .load(path = "data/train_identity.csv")


    // How Many Nulls
    trans.percentNulls()
      .withColumn("id", F.lit(1))
      .melt(id_vars = Seq("id"),
            value_vars = trans.columns)
      .drop("id")
      .orderBy(sortExprs = F.col("value").desc)
      .show(1000)


    trans.removeNullRows

}
