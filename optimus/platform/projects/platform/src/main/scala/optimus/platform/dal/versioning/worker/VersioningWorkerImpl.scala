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
package optimus.platform.dal.versioning.worker

import java.rmi.registry.LocateRegistry
import java.rmi.registry.Registry
import java.rmi.server.UnicastRemoteObject

import msjava.slf4jutils.scalalog.getLogger
import optimus.config.RuntimeConfiguration
import optimus.entity.EntityInfoRegistry
import optimus.graph.DiagnosticSettings
import optimus.platform.dsi.bitemporal.TransformerNotFoundException
import optimus.platform.dsi.bitemporal.proto.TemporalContextDeserializer
import optimus.platform.versioning.TransformerRegistry
import optimus.platform.versioning.worker._

import scala.util.Try

private[optimus] class VersioningWorkerImpl private (runtimeConfig: RuntimeConfiguration, registry: Registry)
    extends VersioningWorker {
  import VersioningWorkerImpl._

  private val executor = new VersioningWorkerPool(runtimeConfig)

  def ipcPerformTransformation(req: VersioningRequest): VersioningResponse = {
    log.debug(s"from server:${req.toString}")
    val tStart = System.currentTimeMillis()
    val result = Try {
      // loading the companion: contract is that transformer registration must take place there so initializing the
      // class should be enough to make sure that happens
      EntityInfoRegistry.getCompanion(req.fromShape.className)
      executor.exec(req.sessionInfo) {
        val deserializeLoadContext = TemporalContextDeserializer.deserialize(req.loadContext)
        TransformerRegistry
          .versionUsingRegisteredFieldType(req.properties, req.fromShape, req.toShape, deserializeLoadContext)
          .getOrElse(throw new TransformerNotFoundException(
            s"fromShape:${req.fromShape} toShape:${req.toShape} loadContext:${req.loadContext}"))
      }
    }
    val tEnd = System.currentTimeMillis()
    val response = new VersioningResponse(result, req.id)
    log.debug(s"to server ${response.toString} t_ex:${(tEnd - tStart).toString}ms")
    response
  }

  def shutdown(): Unit = {
    try {
      log.info("executing versioning worker shutdown")
      registry.unbind(VersioningWorkerRegistryLocatorId.Id)
      UnicastRemoteObject.unexportObject(this, true)
      executor.close()
      log.info("successful versioning worker shutdown")
    } catch {
      case ex: Exception =>
        log.error(s"exception during versioning worker shutdown: ${ex.getMessage}", ex)
        throw ex
    }
  }
}

private[optimus] object VersioningWorkerImpl {
  val log = getLogger(this)

  lazy val registryPort =
    DiagnosticSettings.getIntProperty("optimus.dal.versioning.worker.registryPort", Registry.REGISTRY_PORT)
  lazy val registry: Registry = LocateRegistry.createRegistry(registryPort)

  def apply(baseRuntimeConfig: RuntimeConfiguration, registry: Registry): VersioningWorker = {
    require(registry.list().isEmpty, "VersioningWorker already registered, can't have more than 1")
    try {
      val worker = new VersioningWorkerImpl(baseRuntimeConfig, registry)
      val stub = UnicastRemoteObject.exportObject(worker, 0)
      registry.bind(VersioningWorkerRegistryLocatorId.Id, stub)
      worker
    } catch {
      case ex: Exception =>
        log.error(s"exception during versioning worker startup:${ex.getMessage}", ex)
        throw ex
    }
  }

  def apply(baseRuntimeConfig: RuntimeConfiguration): VersioningWorker = apply(baseRuntimeConfig, registry)
}
