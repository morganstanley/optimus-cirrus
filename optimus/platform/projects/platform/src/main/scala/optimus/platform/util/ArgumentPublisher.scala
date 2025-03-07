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
package optimus.platform.util

import scala.jdk.CollectionConverters._
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Crumb.RuntimeSource
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.Properties.Elems
import optimus.breadcrumbs.crumbs.Properties.jenkinsAppName
import optimus.breadcrumbs.crumbs.Properties.mergedAppName
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.graph.diagnostics.sampling.SamplingProfilerSource
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.NamedOptionDef

import java.lang.management.ManagementFactory

object ArgumentPublisher {

  val JavaArg = "java"
  val OptimusArg = "optimus"
  val OptimusEnvArg = "optimusenv"
  val ExtractedArg = "extracted"

  private def javaOpts: Map[String, String] = {

    parseJavaArgs(ManagementFactory.getRuntimeMXBean.getInputArguments.asScala)
  }

  private def optimusEnv: Map[String, String] = {
    val interesting = Seq("OPTIMUS", "MALLOC", "LD_PRELOAD")
    System.getenv().asScala.filter { case (k, _) => interesting.exists(k.contains(_)) }.toMap
  }

  private[util] def searchAppNameFromArgs(
      extArgs: Map[String, String],
      javaArgs: Map[String, String],
      optimusArgs: Map[String, String]): String = {
    val jenkinsName = extArgs.get(jenkinsAppName.toString)
    val runconfName = javaArgs.get("-Drunconf.name")
    val appName = optimusArgs.get("app-name")
    val app = optimusArgs.get("app")
    val allNames = Seq(runconfName, app, appName, jenkinsName)
    allNames match {
      case Seq(None, None, None, None) => ""
      case _ =>
        val resolvedNames = allNames.flatten.map(_.replaceAll("\"", ""))
        val validLongNames =
          resolvedNames.filterNot(name =>
            resolvedNames.exists(other =>
              other.toLowerCase != name.toLowerCase && other.toLowerCase.contains(name.toLowerCase)))
        validLongNames.mkString(", ")
    }
  }

  private def extracted: Map[String, String] = Version.properties.toMap.map { case (k, v) =>
    k -> v.toString().replaceAll("\"", "")
  }

// Divides the list of tokens into batches such that the sum of the lengths in each
// batch doesn't exceed maxLength
  private def batchTokens(args: Map[String, String], maxLength: Int): Seq[Map[String, String]] = args
    .foldLeft[(List[Map[String, String]], Int)]((List(Map.empty[String, String]), 0)) {
      case ((Nil, _), _) =>
        throw new RuntimeException("This can't happen")
      case ((currBatch :: earlierBatches, batchLen), (key, value)) =>
        val argLength = key.length + value.length
        val newLen = batchLen + argLength
        if (newLen < maxLength) // add arg to current batch
          ((currBatch + (key -> value)) :: earlierBatches, newLen)
        else // start a new batch
          (Map(key -> value) :: currBatch :: earlierBatches, argLength)
    }
    ._1
    .reverse

  val crumbSource = RuntimeSource + SamplingProfilerSource

  private val maxCharCount = 5000

  private def publish(argType: String, args: Map[String, String], versionProperties: Elems): Unit = {
    val batches = batchTokens(args, maxCharCount)
    batches.zipWithIndex.foreach { case (batch, i: Int) =>
      Breadcrumbs.info(
        ChainedID.root,
        PropertiesCrumb(
          _,
          crumbSource,
          Properties.crumbType -> "CommandLine" ::
            Properties.argsType -> argType ::
            Properties.argsMap -> batch ::
            Properties.batchId -> (i + 1) :: versionProperties
        )
      )
    }
  }

  def parseOptimusArgsToMap(args: Seq[String], parser: CmdLineParser): Map[String, String] = {
    val allKeys =
      if (parser != null)
        parser.getOptions.asScala
          .flatMap {
            _.option match {
              case namedOptionDef: NamedOptionDef => namedOptionDef.name() +: namedOptionDef.aliases().toSeq
              case _                              => Seq.empty
            }
          }
          .filter(_.nonEmpty)
          .toSet
      else args.filter(_.startsWith("-")).toSet
    val defaultValue = "true"
    args
      .foldLeft(Seq.empty[(String, String)]) {
        case (acc, k) if allKeys.contains(k) =>
          acc.filterNot(_._1 == k) :+ (k -> defaultValue)
        case (acc, nextV) =>
          acc.lastOption match {
            case Some((lastK, v)) if allKeys.contains(lastK) =>
              if (v == defaultValue) acc.init :+ (lastK -> nextV)
              else acc.init :+ (lastK -> (v + "," + nextV))
            case _ => acc :+ (nextV -> defaultValue)
          }
      }
      .toMap
  }

  // keep in sync with CoreHelpers.java:inputArgsMap
  def parseJavaArgs(javaArgs: Seq[String]): Map[String, String] =
    javaArgs
      .collect {
        case arg if arg.trim.nonEmpty =>
          val parts = arg.trim.split("=", 2)
          if (parts.length == 2) parts(0) -> parts(1)
          else parts(0) -> ""
      }
      .groupBy(_._1)
      .map { case (k, v) => k -> v.map(_._2).last }

  def publishArgs(programArgs: Iterable[String], parser: CmdLineParser): Unit = {
    val optimusArgs = parseOptimusArgsToMap(programArgs.toSeq, parser)
    val appName = searchAppNameFromArgs(extracted, javaOpts, optimusArgs)
    val versionProperties: Elems = mergedAppName -> appName :: Version.properties
    publish(JavaArg, javaOpts, versionProperties)
    publish(OptimusArg, optimusArgs, versionProperties)
    publish(OptimusEnvArg, optimusEnv, versionProperties)
    publish(ExtractedArg, extracted, versionProperties)
  }
}
