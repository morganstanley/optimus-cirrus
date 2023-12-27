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

import freemarker.cache.StringTemplateLoader
import freemarker.template.Configuration
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.ProcessorArtifactType
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.RelativePath
import optimus.platform._
import optimus.platform.util.Log

import java.io.StringWriter
import java.util
import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters._

@entity class FreemarkerProcessor extends TemplatingProcessor {
  override val artifactType: ProcessorArtifactType = ArtifactType.Freemarker
  override protected def generateContentFromTemplate(
      scopeId: ScopeId,
      processorName: String,
      templateFile: RelativePath,
      classLoader: ClassLoader,
      objects: Seq[Any],
      configuration: Map[String, String],
      templateWithHeaderAndFooter: String): String = {

    FreemarkerProcessor.generateContent(templateFile, classLoader, objects, configuration, templateWithHeaderAndFooter)
  }
}

object FreemarkerProcessor extends Log {

  def generateContent(
      templateFile: RelativePath,
      classLoader: ClassLoader,
      objects: Seq[Any],
      configuration: Map[String, String],
      templateWithHeaderAndFooter: String): String = {

    val templateLoader = new StringTemplateLoader()
    templateLoader.putTemplate(templateFile.pathString, templateWithHeaderAndFooter)

    val freemarkerConfig = new Configuration(Configuration.VERSION_2_3_23)
    freemarkerConfig.setTemplateLoader(templateLoader)

    val template = freemarkerConfig.getTemplate(templateFile.pathString)

    val context = new util.HashMap[String, Object]()
    // unfortunately velocity does not support scala collections, so we need to use java ones instead
    context.put("objects", objects.asJava)
    context.put("classLoader", classLoader)
    configuration.foreach { case (k, v) => context.put(k, v) }
    val sw = new StringWriter()
    template.process(context, sw)
    sw.toString
  }
}
