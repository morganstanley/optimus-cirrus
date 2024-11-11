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
package optimus.buildtool.tools

import optimus.buildtool.app.OptimusBuildToolAppBase
import optimus.buildtool.builders.postbuilders.metadata._
import optimus.buildtool.builders.reporter.JsonReporter
import optimus.buildtool.files.Directory
import optimus.buildtool.resolvers.YamlResolver.loadYaml
import optimus.platform._
import optimus.utils.app.PathOptionOptionHandler
import org.kohsuke.args4j
import org.kohsuke.args4j.spi.PathOptionHandler
import org.kohsuke.args4j.spi.StringOptionHandler

import java.nio.file.Path
import java.nio.file.Paths
import scala.io.Source
import scala.util.Using

class WebDependencyResolverToolCmdLine extends OptimusAppCmdLine {
  @args4j.Option(
    name = "--meta",
    required = true,
    handler = classOf[StringOptionHandler],
    usage = "AFS Meta"
  )
  val meta: String = ""

  @args4j.Option(
    name = "--project",
    required = true,
    handler = classOf[StringOptionHandler],
    usage = "AFS Meta"
  )
  val project: String = ""

  @args4j.Option(
    name = "--release",
    required = true,
    handler = classOf[StringOptionHandler],
    usage = "AFS Release"
  )
  val release: String = ""

  @args4j.Option(
    name = "--build-id",
    required = true,
    handler = classOf[StringOptionHandler],
    usage = "Build ID for CI run"
  )
  val buildId: String = ""

  @args4j.Option(
    name = "--pnpm-lock-file",
    required = true,
    handler = classOf[PathOptionHandler],
    usage = "Path to yaml file containing pnpm dependencies"
  )
  val pnpmLockFile: Path = Paths.get("pnpm-lock.yaml")

  @args4j.Option(
    name = "--output-dir",
    handler = classOf[PathOptionOptionHandler],
    usage = "Path where metadata json file will be generated"
  )
  val outputDir: Option[Path] = None
}

private[buildtool] object WebDependencyResolverTool
    extends OptimusApp[WebDependencyResolverToolCmdLine]
    with OptimusBuildToolAppBase[WebDependencyResolverToolCmdLine] {
  @entersGraph override def run(): Unit =
    main(cmdLine.pnpmLockFile, cmdLine.meta, cmdLine.project, cmdLine.project, cmdLine.buildId, cmdLine.outputDir)

  private def readYaml(pnpmLockFile: Path): String = Using(Source.fromFile(pnpmLockFile.toFile))(_.mkString)
    .getOrThrow(s"I/O issues reading $pnpmLockFile")

  private def generateMetaBundleReport(
      meta: String,
      project: String,
      release: String,
      buildId: String,
      dependencies: Set[DependencyReport]): MetaBundleReport =
    MetaBundleReport(
      metadataCreator = "optimus/buildtool/obt-latest",
      metadataVersion = "1.0",
      meta = meta,
      project = project,
      release = release,
      buildInfo = BuildInfoReport(buildId),
      artifacts = Set(
        ArtifactReport(
          name = project,
          tpe = ".trainsubproject",
          scope = "public",
          usage = s"group: '$meta.$project'",
          onToolchain = Map("*" -> DependenciesReport(dependencies))
        ))
    )

  @async def main(
      pnpmLockFile: Path,
      meta: String,
      project: String,
      release: String,
      buildId: String,
      outputDir: Option[Path]): MetaBundleReport = {
    log.info(s"Parsing dependencies from $pnpmLockFile...")
    val pnpmYamlComment = readYaml(pnpmLockFile)

    main(pnpmYamlComment, meta, project, release, buildId, outputDir)
  }

  @async def main(
      pnpmYamlComment: String,
      meta: String,
      project: String,
      release: String,
      buildId: String,
      outputDir: Option[Path]): MetaBundleReport = {
    val webDependencies = loadYaml(pnpmYamlComment)
    log.info(s"Found ${webDependencies.size} web dependencies")

    val dependencies = webDependencies.apar.map(DependencyReport.fromWebDependency(_, Set(Compile)))
    val metaBundleReport = generateMetaBundleReport(meta, project, release, buildId, dependencies)

    log.info(s"Meta bundle report generated for $meta/$project/$release")
    outputDir match {
      case Some(outputDir) =>
        val metadataJson = s"$meta.$project-metadata.json"
        JsonReporter.writeJsonFile(Directory(outputDir), metaBundleReport, metadataJson)
        log.info(s"Meta bundle report written to ${outputDir.resolve(metadataJson)}")
      case None => log.info(metaBundleReport.toString)
    }
    metaBundleReport
  }
}
