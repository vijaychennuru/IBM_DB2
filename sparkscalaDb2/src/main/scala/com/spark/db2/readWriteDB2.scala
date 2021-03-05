package com.spark.db2

import org.apache.spark.sql.SparkSession
import java.sql.DriverManager
import java.util.Properties
import scala.io.Source
import scala.util.control.Exception._

object readWriteDB2 {

  val spark = SparkSession
    .builder()
    .appName("Connect IBM COS")
    .config("spark.sql.shuffle.partitions", 10)
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  def getProp(): Properties = {

    // reading the property file externally
    val url = getClass.getResource("application.properties")
    val prop: Properties = new Properties()
    if (url != null) {
      val source = Source.fromURL(url)
      prop.load(source.bufferedReader())
    } else {
      print("properties file cannot be loaded at path ")
    }
    return prop
  }

  def main(args: Array[String]) {

    val prop = getProp()
    val DB2_CONNECTION_URL = prop.getProperty("url")

    Class.forName("com.ibm.db2.jcc.DB2Driver")
    prop.put("spark.sql.dialect", "sql")

    //reading data from local path
    val path = getClass.getResource("employee_details.csv").getPath
    val df = spark.read.option("header", "true").csv(path)

    df.show()

    //writing the above read data into IBM DB2
    df.write.jdbc(DB2_CONNECTION_URL, "msc17587.emp_details", prop)

    //Reading back the table from DB2
    val df2 = spark.read.jdbc(DB2_CONNECTION_URL, "msc17587.emp_details", prop)
    df2.show()

    try {
      // registering the table as a temporary table for sql operations
      df.registerTempTable("emp")

      val ratio_df = spark.sql(""" select DEPTNO, 
    sum(case when gender = 'MALE' then 1 else 0 end)/count(*) male_ratio, 
    sum(case when gender = 'FEMALE' then 1 else 0 end)/count(*) fem_ratio 
    from emp group by DEPTNO order by count(*) desc """)

      val avg_df = spark.sql(" select deptno, avg(sal) average_salary from emp group by deptno ")

      val salary_gap_df = spark.sql(""" select DEPTNO, 
    round(avg(case when gender='MALE' then sal end),0) avg_m_salary, 
    round(avg(case when gender='FEMALE' then sal end),0) avg_f_salary, 
    (round(avg(case when gender='MALE' then sal end),0) - round(avg(case when gender='FEMALE' then sal end),0) ) diff_in_avg 
    from emp group by DEPTNO order by DEPTNO """)

      // writing the above genrated dataset into DB2 instance
      ratio_df.write.jdbc(DB2_CONNECTION_URL, "msc17587.gender_ratios", prop)
      avg_df.write.jdbc(DB2_CONNECTION_URL, "msc17587.salary_average", prop)
      salary_gap_df.write.jdbc(DB2_CONNECTION_URL, "msc17587.salary_gap", prop)
    } catch {
      case _: RuntimeException => println("caught a Runtime Exception")
      case _: Exception        => println("Unkown Exception")
    }

  }
}