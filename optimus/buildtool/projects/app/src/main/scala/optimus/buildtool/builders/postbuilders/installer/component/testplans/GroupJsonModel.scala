/*
 * Morgan Stanley makes this available to you under the Apache License, Version 2.0 (the "License").
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package optimus.buildtool.builders.postbuilders.installer.component.testplans

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.ModuleId
import optimus.buildtool.config.StaticConfig
import optimus.buildtool.config.TestplanConfiguration
import optimus.buildtool.config.VersionConfiguration
import optimus.buildtool.format.WorkspaceStructure
import optimus.utils.CollectionUtils._

import scala.collection.Set
import scala.collection.immutable.Seq

object TestType {
  private val defaultAdditionalBindings: Map[String, String] = Map(
    // Default timeout for all tests that do not declare the custom one
    "TEST_RUNNER_EXTRA_OPTS" -> "--timeout 120",
    "OWNER" -> StaticConfig.string("ciUserName")
  )

  private def defaultTreadmillOpts(testplanConfig: TestplanConfiguration): Map[String, String] =
    Map(
      "cell" -> testplanConfig.cell,
      "cpu" -> testplanConfig.cpu,
      "disk" -> testplanConfig.disk,
      "mem" -> testplanConfig.mem,
      "priority" -> testplanConfig.priority.toString
    )

  private val allowedTestGroups: Set[String] = Set(
    "test",
    "functionalTest",
    "functionalUnstableTest",
    "integrationTest",
    "smokePassFailTest",
    "uiTest"
  )
}

/**
 * Classes here are used to parse .json files in /profiles/testGroups.
 */
final case class TestType(
    testGroups: Seq[Group],
    testGroupsOpts: TestGroupOpts
) {

  import TestType._

  def mergeWith(testType: TestType): TestType =
    TestType(testGroups ++ testType.testGroups, testGroupsOpts.mergeWith(testType.testGroupsOpts))

  def bindingsFor(
      testGroup: Group,
      versionConfig: VersionConfiguration,
      testplanConfig: TestplanConfiguration,
      workspaceStructure: WorkspaceStructure): Map[String, String] = {
    bindingsFromConfig(versionConfig, testplanConfig.useTestCaching) ++
      defaultAdditionalBindings ++
      testGroup.enrichedSingleTestOwner(workspaceStructure) ++
      // let test group override enriched owner, if set
      testGroupsOpts.additionalBindings.getOrElse(Map.empty) ++
      testGroup.additionalBindings.getOrElse(Map.empty)
  }.ensuring(
    additionalBinding =>
      additionalBinding("OWNER") != StaticConfig.string("ciUserName") ||
        allowedTestGroups.contains(testGroupsOpts.testGroupsName),
    s"""No owner for test group ${testGroup.name} defined in ${testGroupsOpts.testGroupsName}.
       |Please set a value for ["additionalBindings"]["OWNER"] key.""".stripMargin
  )

  def displayName: String =
    if (testGroupsOpts.testGroupsName == "test") "UnitTest"
    else testGroupsOpts.testGroupsName.capitalize

  def treadmillOptsFor(testGroup: Group, testplanConfig: TestplanConfiguration): Map[String, String] = {
    defaultTreadmillOpts(testplanConfig) ++
      testGroupsOpts.treadmillOpts.getOrElse(Map.empty) ++
      testGroup.treadmillOpts.getOrElse(Map.empty)
  }

  def estimates: Map[String, Int] = testGroupsOpts.maxNoContainers match {
    case Some(value) if value > 1 =>
      createEstimatedNoContainers(value, testGroupsOpts.maxTestsInGroups.getOrElse(Int.MaxValue), testGroups)
    case _ =>
      Map.empty[String, Int]
  }

  private def bindingsFromConfig(versionConfig: VersionConfiguration, useDtc: Boolean): Map[String, String] = Map(
    "PROJECT_VERSION" -> versionConfig.installVersion,
    "OBT_VERSION" -> versionConfig.obtVersion,
    "SCALA_VERSION" -> versionConfig.scalaVersion,
    "DTC_FLAG" -> (if (useDtc) "--enabledtc" else "")
  )

  private def createEstimatedNoContainers(
      maxNoContainers: Int,
      maxTestsInContainer: Int,
      testGroups: Seq[Group]): Map[String, Int] = {
    val groupSizes: Map[String, Int] = testGroups
      .map(group => group.name -> group.entries.size)
      .filter { case (_, groupSize) => groupSize > maxTestsInContainer }
      .toMap

    val allTests = groupSizes.values.sum

    groupSizes.map { case (groupName, groupSize) =>
      val estimate = Math.round(groupSize.toFloat * maxNoContainers / allTests)
      val testsInContainer = groupSize / estimate
      require(
        testsInContainer < maxTestsInContainer,
        s"Cannot satisfy the maxTestsInContainer: $maxTestsInContainer with maxNoContainers: $maxNoContainers")
      groupName -> estimate
    }
  }
}

final case class TestGroupOpts(
    testGroupsName: String, // eg. uiTest - matches the name in .runconf file
    testModulesFileName: String,
    maxNoContainers: Option[Int],
    maxTestsInGroups: Option[Int],
    additionalBindings: Option[Map[String, String]],
    treadmillOpts: Option[Map[String, String]],
    private val platform: Option[String] // forcing people to use 'os' here, as defaults are unfortunately not supported
) {

  val os: String = this.platform.getOrElse("unix")

  def mergeWith(that: TestGroupOpts): TestGroupOpts = {
    require(
      this.testGroupsName == that.testGroupsName,
      s"Cannot merge test types with different names, got ${this.testGroupsName} and ${that.testGroupsName}")
    require(
      this.testModulesFileName == that.testModulesFileName,
      s"Cannot merge test types with different file names, got ${this.testModulesFileName} and ${that.testModulesFileName}"
    )
    def msg(name: String) = s"$testGroupsName: cannot override $name, please define it in only one file"
    require(this.maxNoContainers.isEmpty || that.maxNoContainers.isEmpty, msg("maxNoContainers"))
    require(this.maxTestsInGroups.isEmpty || that.maxTestsInGroups.isEmpty, msg("maxTestsInGroups"))
    require(this.additionalBindings.isEmpty || that.additionalBindings.isEmpty, msg("additionalBindings"))
    require(this.treadmillOpts.isEmpty || that.treadmillOpts.isEmpty, msg("treadmillOpts"))
    require(this.platform.isEmpty || that.platform.isEmpty, msg("platform"))

    this.copy(
      maxNoContainers = this.maxNoContainers.orElse(that.maxNoContainers),
      maxTestsInGroups = this.maxTestsInGroups.orElse(that.maxTestsInGroups),
      additionalBindings = this.additionalBindings.orElse(that.additionalBindings),
      treadmillOpts = this.treadmillOpts.orElse(that.treadmillOpts)
    )
  }
}

@JsonDeserialize(using = classOf[EntryDeserializer])
@JsonSerialize(using = classOf[EntrySerializer])
final case class Entry(moduleId: ModuleId, testName: Option[String])

class EntrySerializer extends StdSerializer[Entry](classOf[Entry]) {

  override def serialize(entry: Entry, gen: JsonGenerator, provider: SerializerProvider): Unit = {
    val name = entry.testName match {
      case Some(t) => s"${entry.moduleId.properPath}.$t"
      case None    => entry.moduleId.properPath
    }
    gen.writeString(name)
  }
}

class EntryDeserializer extends StdDeserializer[Entry](classOf[Entry]) {

  override def deserialize(jsonParser: JsonParser, ctxt: databind.DeserializationContext): Entry = {
    val text = jsonParser.getText
    text.split('.').toIndexedSeq match {
      case Seq(meta, bundle, module, testName) => Entry(ModuleId(meta, bundle, module), Some(testName))
      case Seq(meta, bundle, module)           => Entry(ModuleId(meta, bundle, module), None)
      case _                                   => throw new RuntimeException(s"Invalid testplan entry: $text")
    }
  }

}

object ProgrammingLanguage extends Enumeration {
  type ProgrammingLanguage = Value
  val Scala, Java, Python = Value

  def fromString(s: String): ProgrammingLanguage =
    values.find(_.toString.equalsIgnoreCase(s)).getOrElse(ProgrammingLanguage.Scala)
}

final case class Group(
    name: String, // eg. examples_ui_api_1
    @JsonProperty("projects") entries: Seq[Entry],
    treadmillOpts: Option[Map[String, String]],
    additionalBindings: Option[Map[String, String]],
    private val lang: String = "Scala"
) {
  import ProgrammingLanguage.ProgrammingLanguage
  val programmingLanguage: ProgrammingLanguage = ProgrammingLanguage.fromString(lang)

  def metaBundle: MetaBundle = entries.map(_.moduleId.metaBundle).distinct match {
    case Seq(mb) => mb
    case _       => throw new IllegalArgumentException(s"No single metabundle for modules: $entries")
  }

  def enrichedSingleTestOwner(workspaceStructure: WorkspaceStructure): Map[String, String] = {
    val uniqueOwners = entries.flatMap { entry =>
      workspaceStructure.modules.get(entry.moduleId).map(_.owningGroup.split(',').head)
    }.distinct

    if (uniqueOwners.size == 1) {
      Map("OWNER" -> uniqueOwners.single)
    } else Map.empty
  }
}
