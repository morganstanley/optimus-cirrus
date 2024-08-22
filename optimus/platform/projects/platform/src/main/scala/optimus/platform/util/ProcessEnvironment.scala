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

import com.fasterxml.jackson.core.util.DefaultIndenter
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectWriter
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import optimus.platform.AgentInfo

import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import scala.collection.compat._
import scala.jdk.CollectionConverters._
import scala.util.Try

final case class OptimusAppInfo(
    main: String,
    agentInfo: List[String]
)
final case class JVM(
    name: String,
    vendor: String,
    version: String,
    javaOpts: Map[String, String]
)

final case class ProcessEnvironment(
    id: String, // chained id or something else?
    app: OptimusAppInfo,
    jvm: JVM,
    args: List[String],
    env: Map[String, String]
)

object ProcessEnvironment {
  private lazy val bean = ManagementFactory.getRuntimeMXBean

  private lazy val inputArguments: Seq[String] = ManagementFactory.getRuntimeMXBean.getInputArguments.asScala
  private lazy val entityAgentArgument: String =
    inputArguments.find(a => a.startsWith("-javaagent:") && a.contains("entityagent")).getOrElse("")
  private lazy val aliasPackageArgument: String =
    inputArguments.find(a => a.startsWith("-Doptimus.packageAliases")).getOrElse("")

  def create(id: String, mainClass: String): ProcessEnvironment = {
    import scala.jdk.CollectionConverters._

    ProcessEnvironment(
      id = id,
      app = OptimusAppInfo(mainClass, agentInfo = AgentInfo.agentInfo().asScala.to(List)),
      jvm = JVM(bean.getVmName, bean.getVmVendor, bean.getVmVersion, sys.props.toMap),
      args = bean.getInputArguments.asScala.to(List),
      env = sys.env
    )
  }

  lazy val javaOptsToInherit: Seq[String] = aliasPackageArgument +: entityAgentArgument +: moduleArguments

  lazy val moduleArguments: Seq[String] = {
    val ourOpts = bean.getInputArguments.asScala.to(Seq)
    ourOpts.filter(o => o.startsWith("--add-exports") || o.startsWith("--add-opens"))
  }

  def logToTmp(id: String, mainClass: String): Try[Path] = Try {
    val path = Files.createTempFile(s"launch-${mainClass}", ".json")
    val writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)
    try ProcessEnvironment.jsonWriter.writeValue(writer, create(id, mainClass))
    finally writer.close()
    path
  }

  private[util] def jsonWriter: ObjectWriter = {
    val mapper = new ObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
    val printer = new DefaultPrettyPrinter()
    printer.indentArraysWith(DefaultIndenter.SYSTEM_LINEFEED_INSTANCE)
    mapper.writer(printer)
  }

}
