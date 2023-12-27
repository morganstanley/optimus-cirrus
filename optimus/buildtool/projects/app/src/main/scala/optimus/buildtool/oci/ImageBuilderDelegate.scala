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
package optimus.buildtool
package oci

import com.google.cloud.tools.jib.api._
import buildplan._
import optimus.buildtool.format.docker.ImageLocation
import optimus.buildtool.utils.BlockingQueue
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

/** A façade over Jib's APIs so we can make (trivial) assertions about the results thereof. */
trait ImageBuilderDelegate[Result] {
  def addFileLayerBuilder(builder: FileEntriesLayer.Builder): Unit
  def addEnvVariable(name: String, value: String): Unit
  def finish(): Result
}

object ImageBuilderDelegate {
  private val log = LoggerFactory.getLogger(getClass)

  type Result = (List[FileEntriesLayer], List[(String, String)])

  /** Create an [[ImageBuilderDelegate]] which stores the provided file layers and env variables in a list. */
  def mock: ImageBuilderDelegate[Result] =
    new ImageBuilderDelegate[Result] {
      private val layers = List.newBuilder[FileEntriesLayer]
      private val envVariables = List.newBuilder[(String, String)]
      def addFileLayerBuilder(builder: FileEntriesLayer.Builder): Unit = {
        layers.synchronized(layers += builder.build())
      }
      def addEnvVariable(name: String, value: String): Unit = envVariables.synchronized(envVariables += name -> value)
      def finish(): Result = layers.result() -> envVariables.result()
    }

  /** Create an [[ImageBuilderDelegate]] which actually builds an image. */
  def real(from: Option[ImageLocation], to: ImageLocation)(
      initJib: JibContainerBuilder => Unit,
      initContainerizer: Containerizer => Unit
  ): ImageBuilderDelegate[JibContainer] = new ImageBuilderDelegate[JibContainer] {

    val layerBuilders: BlockingQueue[FileEntriesLayer.Builder] = new BlockingQueue()

    val jib: JibContainerBuilder = from.map(_.mkBuilder(initJib)).getOrElse {
      val init = Jib.fromScratch
      initJib(init)
      init
    }
    val containerizer: Containerizer = to.mkContainerizer(initContainerizer)

    def addEnvVariable(name: String, value: String): Unit = jib.addEnvironmentVariable(name, value)

    def addFileLayerBuilder(builder: FileEntriesLayer.Builder): Unit = layerBuilders.put(builder)

    def finish(): JibContainer = {
      val builders = layerBuilders.pollAll()

      def buildLayersAndTar = {
        // [1] it builds layers and computes the size of each file
        builders.foreach(builder => jib.addFileEntriesLayer(builder.build()))

        // [2] it builds the actual tar
        // unfortunately this will fail if any file has changed since [1]
        jib.containerize(containerizer)
      }

      try buildLayersAndTar
      catch {
        // This could happen if files changed while building the layers and tar
        case NonFatal(ex) if ex.getMessage.contains("bytes exceeds size in header") =>
          log.warn(
            s"Files have changed during the docker build for image ${to.name}. Let's trying again before giving up...",
            ex)
          buildLayersAndTar
      }

    }
  }

}
