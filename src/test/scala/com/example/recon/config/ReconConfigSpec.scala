package com.example.recon.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ReconConfigSpec extends AnyFlatSpec with Matchers {

  "ColumnMapping" should "parse valid mapping string" in {
    val mapping = ColumnMapping.parse("source_col:target_col")
    mapping should be (Right(ColumnMapping("source_col", "target_col")))
  }

  it should "return Left for invalid mapping string" in {
    ColumnMapping.parse("source_col_target_col").isLeft should be (true)
    ColumnMapping.parse("source_col:").isLeft should be (true)
    ColumnMapping.parse(":target_col").isLeft should be (true)
  }

  it should "parse a list of valid mapping strings" in {
    val (mappings, errors) = ColumnMapping.parseList("src1:tgt1, src2:tgt2 , src3:tgt3")
    errors should be (empty)
    mappings should contain theSameElementsAs List(
      ColumnMapping("src1", "tgt1"),
      ColumnMapping("src2", "tgt2"),
      ColumnMapping("src3", "tgt3")
    )
  }

  it should "handle empty mapping string list" in {
    val (mappings, errors) = ColumnMapping.parseList("")
    errors should be (empty)
    mappings should be (empty)
  }

  it should "collect errors for invalid items in a list" in {
    val (mappings, errors) = ColumnMapping.parseList("src1:tgt1,invalid,src2:tgt2,:empty_src")
    mappings should contain theSameElementsAs List(
      ColumnMapping("src1", "tgt1"),
      ColumnMapping("src2", "tgt2")
    )
    errors should have length 2
    errors.head should include ("Invalid column mapping format: 'invalid'")
    errors.last should include ("Invalid column mapping format: ':empty_src'")
  }

  "ReconJobConfig" should "be created with valid parameters" in {
    val config = ReconJobConfig(
      jobName = "testJob",
      sourceType = "FILE",
      sourcePath = "/path/to/source",
      targetTable = "schema.target",
      primaryKeyColumns = List("id"),
      sourceToTargetFlag = "Y",
      businessRuleFlag = "N",
      columnMappingString = "src_id:tgt_id,src_val:tgt_val",
      businessRuleSQL = None,
      outputPath = "/path/to/output",
      fileFormat = Some("csv"),
      fileDelimiter = Some(","),
      fileHasHeader = Some(true)
    )
    config.jobName should be ("testJob")
    config.runSourceToTarget should be (true)
    config.runBusinessRules should be (false)
    config.parsedColumnMappings should have length 2
    config.parsedColumnMappings.head should be (ColumnMapping("src_id", "tgt_id"))
  }

  it should "throw IllegalArgumentException for invalid sourceToTargetFlag" in {
    assertThrows[IllegalArgumentException] {
      ReconJobConfig("test", "FILE", "/src", "tgt", List("id"), "X", "N", "", None, "/out", None, None, None)
    }
  }

  it should "throw IllegalArgumentException for invalid businessRuleFlag" in {
    assertThrows[IllegalArgumentException] {
      ReconJobConfig("test", "FILE", "/src", "tgt", List("id"), "Y", "X", "", None, "/out", None, None, None)
    }
  }

  it should "throw IllegalArgumentException for empty primaryKeyColumns" in {
    assertThrows[IllegalArgumentException] {
      ReconJobConfig("test", "FILE", "/src", "tgt", List(), "Y", "N", "", None, "/out", None, None, None)
    }
  }

  it should "correctly identify if business rules should run" in {
     val configWithSql = ReconJobConfig("test", "FILE", "/src", "tgt", List("id"), "Y", "Y", "", Some("SELECT * FROM source"), "/out", None, None, None)
     configWithSql.runBusinessRules should be (true)

     val configWithoutSql = ReconJobConfig("test", "FILE", "/src", "tgt", List("id"), "Y", "Y", "", None, "/out", None, None, None)
     configWithoutSql.runBusinessRules should be (false)

     val configWithEmptySql = ReconJobConfig("test", "FILE", "/src", "tgt", List("id"), "Y", "Y", "", Some("   "), "/out", None, None, None)
     configWithEmptySql.runBusinessRules should be (false)
  }
}
