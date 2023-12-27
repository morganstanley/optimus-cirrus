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
package optimus.buildtool.processors

import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.ProcessorArtifactType
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.RelativePath
import optimus.platform._
import optimus.platform.util.Log
import org.apache.velocity.VelocityContext
import org.apache.velocity.app.VelocityEngine
import org.apache.velocity.runtime.log.NullLogChute
import org.apache.velocity.runtime.resource.loader.StringResourceLoader

import java.io.StringWriter
import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters._

@entity class VelocityProcessor extends TemplatingProcessor {

  override val artifactType: ProcessorArtifactType = ArtifactType.Velocity

  override protected def generateContentFromTemplate(
      scopeId: ScopeId,
      processorName: String,
      templateFile: RelativePath,
      classLoader: ClassLoader,
      objects: Seq[Any],
      configuration: Map[String, String],
      templateWithHeaderAndFooter: String): String = {

    VelocityProcessor.generateContent(
      scopeId,
      processorName,
      templateFile,
      classLoader,
      objects,
      configuration,
      templateWithHeaderAndFooter)
  }
}

object VelocityProcessor extends Log {

  private def engine(processorKey: String) = {
    val ve = new VelocityEngine()
    ve.setProperty("runtime.references.strict", "true")
    ve.setProperty("runtime.log.logsystem.class", classOf[NullLogChute].getName)
    ve.setProperty("resource.loader", "string")
    ve.setProperty("string.resource.loader.class", "org.apache.velocity.runtime.resource.loader.StringResourceLoader")
    ve.setProperty("string.resource.loader.repository.name", processorKey)
    ve.init()
    ve
  }

  def generateContent(
      scopeId: ScopeId,
      processorName: String,
      templateFile: RelativePath,
      classLoader: ClassLoader,
      objects: Seq[Any],
      configuration: Map[String, String],
      templateWithHeaderAndFooter: String): String = {

    val processorKey = s"$scopeId:$processorName"
    val e = engine(processorKey)
    val repo = StringResourceLoader.getRepository(processorKey)
    repo.putStringResource(templateFile.pathString, templateWithHeaderAndFooter)
    val template = e.getTemplate(templateFile.pathString)

    val context = new VelocityContext()
    // unfortunately velocity does not support scala collections, so we need to use java ones instead
    context.put("objects", objects.asJava)
    context.put("classLoader", classLoader)
    configuration.foreach { case (k, v) => context.put(k, v) }
    val sw = new StringWriter()
    template.merge(context, sw)
    sw.toString
  }

}
