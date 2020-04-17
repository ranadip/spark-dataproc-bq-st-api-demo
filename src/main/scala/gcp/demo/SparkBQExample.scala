package gcp.demo

import com.google.cloud.spark.bigquery._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

object SparkBQExample {

  def main(args: Array[String]): Unit = {
    val sparkBuilder = SparkSession
      .builder
      .appName("Spark BQ Example")

    args.toList.map {
      case "--local" => sparkBuilder.master("local[*]")
    }

    val spark = sparkBuilder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val platinumDeepVariantDf = spark.read.bigquery("bigquery-public-data.human_genome_variants.platinum_genomes_deepvariant_variants_20180823")
      //    val platinumDeepVariantDf = spark.read.bigquery("ran-gcct-prj1.amst.platinum_genomes_deepvariant_variants_20180823_limit_100")
      .select("reference_name", "start_position", "end_position", "reference_bases", "alternate_bases", "call")
      .cache()
    platinumDeepVariantDf.createOrReplaceTempView("platinumDeepVariant")
    //    spark.sql("SELECT * from platinumDeepVariant").show()

    //    platinumDeepVariantDf.show()
    //    platinumDeepVariantDf.printSchema()

    //    val pDF = platinumDeepVariantDf.selectExpr("""replace(reference_name, "chr", "") """)
    val pDF1 = spark.sql(
      """
        |SELECT
        |REPLACE(reference_name, 'chr', '') as reference_name,
        |reference_name as ref_name,
        |start_position,
        |end_position,
        |reference_bases,
--        |alternate_bases.alt AS alt,
        |call,
--        |inline_outer(call)
        |posexplode_outer(alternate_bases)
 --       |COUNT(call.genotype = pos + 1) AS num_variant_alleles
 --       |COUNT(gt >= 0) FROM v.call call, call.genotype gt) AS total_num_alleles
        |--(SELECT COUNT(gt = pos + 1) FROM v.call call, call.genotype gt) AS num_variant_alleles,
        |--(SELECT COUNT(gt >= 0) FROM v.call call, call.genotype gt) AS total_num_alleles
        |FROM platinumDeepVariant
--        |LIMIT 10
        |""".stripMargin)

    pDF1.printSchema()

    pDF1.createOrReplaceTempView("pDF1")

    val pDF2 = spark.sql(
      """
        |SELECT
        |reference_name,
        |ref_name,
        |start_position,
        |end_position,
        |reference_bases,
--        |alt,
        |inline_outer(call),
        |pos as alt_offset,
        |col.alt as alt
        |FROM PDF1
        |""".stripMargin
    )

    //    pDF2.printSchema()

    pDF2.createOrReplaceTempView("pDF2")
    /** PDF2 Schema:
     * reference_name,
     * ref_name,
     * start_position,
     * end_position,
     * reference_bases,
     * alt,
     * name,
     * genotype,
     * phaseset
     * GQ,
     * DP,
     * MIN_DP,
     * AD,
     * VAF,
     * GL,
     * PL,
     * quality,
     * filter,
     * alt_offset,
     * col_alt
     * */

    val pDF3 = spark.sql(
      """
        |SELECT
        |reference_name,
        |start_position,
        |end_position,
        |reference_bases,
        |alt,
        |size(filter(genotype, col -> col == alt_offset + 1)) as num_variant_alleles,
        |size(filter(genotype, col -> col >= 0)) as total_num_alleles
        |FROM PDF2
        |""".stripMargin
    )

    //    pDF3.printSchema()
    //    pDF3.show()
    //    println(pDF3.count())

    pDF3.createOrReplaceTempView("variants")
    /**
     * Schema of variants:
     * root
     * |-- reference_name: string (nullable = true)
     * |-- ref_name: string (nullable = true)
     * |-- start_position: long (nullable = true)
     * |-- end_position: long (nullable = true)
     * |-- reference_bases: string (nullable = true)
     * |-- alt: array (nullable = true)
     * |    |-- element: string (containsNull = false)
     * |-- name: string (nullable = true)
     * |-- gt: long (nullable = false)
     * |-- alt_offset: integer (nullable = true)
     * |-- col_alt: string (nullable = true)
     */


    // Perform word count.
    //    val testDF = spark.sql(
    //      """SELECT
    //        |    REPLACE(reference_name, 'chr', '') as reference_name,
    //        |    reference_name as ref_name,
    //        |    start_position,
    //        |    end_position,
    //        |    reference_bases,
    //        |    alternate_bases.alt AS alt,
    //        |    (SELECT COUNTIF(gt = alt_offset+1) FROM v.call call, call.genotype gt) AS num_variant_alleles,
    //        |    (SELECT COUNTIF(gt >= 0) FROM v.call call, call.genotype gt) AS total_num_alleles
    //        |  FROM platinumDeepVariant, UNNEST(v.alternate_bases) alternate_bases WITH OFFSET alt_offset
    //        |  """.stripMargin)

    //    val intervalDf = spark.sql(
    //      """
    //        |SELECT * FROM UNNEST ([
    //        |    STRUCT<Gene STRING, Chr STRING, gene_start INT64, gene_end INT64, region_start INT64, region_end INT64>
    //        |    ('PRCC', '1', 156736274, 156771607, 156636274, 156871607),
    //        |    ('NTRK1', '1', 156785541, 156852640, 156685541, 156952640),
    //        |    ('PAX8', '2', 113972574, 114037496, 113872574, 114137496),
    //        |    ('FHIT', '3', 59734036, 61238131, 59634036, 61338131),
    //        |    ('PPARG', '3', 12328349, 12476853, 12228349, 12576853)
    //        |  ])
    //        |""".stripMargin
    //    )

    val intervalSchema = new StructType().add("Gene", StringType)
      .add("Chr", StringType)
      .add("gene_start", LongType)
      .add("gene_end", LongType)
      .add("region_start", LongType)
      .add("region_end", LongType)

    val intervalData = Seq(
      Row("PRCC", "1", 156736274L, 156771607L, 156636274L, 156871607L),
      Row("NTRK1", "1", 156785541L, 156852640L, 156685541L, 156952640L),
      Row("PAX8", "2", 113972574L, 114037496L, 113872574L, 114137496L),
      Row("FHIT", "3", 59734036L, 61238131L, 59634036L, 61338131L),
      Row("PPARG", "3", 12328349L, 12476853L, 12228349L, 12576853L)
    )

    val intervalDf = spark.createDataFrame(
      spark.sparkContext.parallelize(intervalData), intervalSchema
    )
    intervalDf.createOrReplaceTempView("intervals")

    /**
     * Schema of intervals
     * root
     * |-- Gene: string (nullable = true)
     * |-- Chr: string (nullable = true)
     * |-- gene_start: long (nullable = true)
     * |-- gene_end: long (nullable = true)
     * |-- region_start: long (nullable = true)
     * |-- region_end: long (nullable = true)
     * */

    //    intervalDf.show()
    //    intervalDf.printSchema()

    val gene_variants = spark.sql(
      """
        |SELECT
        |    reference_name,
        |    start_position,
        |    reference_bases,
        |    alt,
        |    num_variant_alleles,
        |    total_num_alleles
        |  FROM
        |    variants
        |  INNER JOIN
        |    intervals ON
        |    variants.reference_name = intervals.Chr
        |    AND intervals.region_start <= variants.start_position
        |    AND intervals.region_end >= variants.end_position
        |""".stripMargin
    )

    gene_variants.createOrReplaceTempView("gene_variants")

    //    gene_variants.show()


    val hg19 = spark.read.bigquery("silver-wall-555.TuteTable.hg19")
      //    val hg19 = spark.read.bigquery("ran-gcct-prj1.amst.hg19")
      .select("Chr", "Start", "Ref", "Alt", "Func", "Gene", "ExonicFunc", "PopFreqMax")
      //        .limit(10)
      .cache()
    hg19.createOrReplaceTempView("hg19")

    //    hg19.printSchema()
    //    hg19.show(10)

    val result = spark.sql(
      """
        |SELECT DISTINCT
        |  Chr,
        |  annots.Start AS Start,
        |  Ref,
        |  annots.Alt,
        |  Func,
        |  Gene,
        |  PopFreqMax,
        |  ExonicFunc,
        |  num_variant_alleles,
        |  total_num_alleles
        |FROM
        |  hg19 AS annots
        |INNER JOIN
        |  gene_variants AS vars
        |ON
        |  vars.reference_name = annots.Chr
        |  AND vars.start_position = annots.Start
        |  AND vars.reference_bases = annots.Ref
        |  AND vars.alt = annots.Alt
        |WHERE
        |  -- Retrieve annotations for rare variants only.
        |  PopFreqMax <= 0.01
        |ORDER BY
        |  Chr,
        |  Start
        |""".stripMargin
    )

    result.printSchema()

    result.show(200)

    spark.stop()
  }
}
