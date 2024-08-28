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
package optimus.platform.dtc

import optimus.platform.dtc.CachedExecutionPlugin.ExecutionResult
import optimus.platform.dtc.CachedExecutionPlugin.ExitHandler

import java.util.ServiceLoader
import scala.jdk.CollectionConverters._

trait CachedExecutionPlugin {

  def execute(appClass: Class[_], args: Array[String], defaultExitHandler: ExitHandler): ExecutionResult

  def isInMemory: Boolean
}

object CachedExecutionPlugin {
  type ExitHandler = (Option[Exception], Long, Int, Boolean) => Unit

  private val pluginServiceLoader = ServiceLoader.load(classOf[CachedExecutionPlugin])

  sealed trait ExecutionResult

  final case object ExecutionCached extends ExecutionResult

  final case object CachingDisabled extends ExecutionResult

  final case class NoCacheFound(cachingExitHandler: ExitHandler, shouldExitOnCacheMiss: Boolean) extends ExecutionResult

  def discover(inMemory: Boolean): List[CachedExecutionPlugin] = {
    pluginServiceLoader.iterator().asScala.toList.filter(_.isInMemory == inMemory)
  }
}
