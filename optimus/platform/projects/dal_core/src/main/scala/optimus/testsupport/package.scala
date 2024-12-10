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
package optimus

import java.util.concurrent.atomic.AtomicBoolean

// import msjava.base.slr.internal.ServiceEnvironment
import optimus.graph.DiagnosticSettings

import scala.util.Try

package object testsupport {

  private val allowBackendAccess = DiagnosticSettings.getBoolProperty("optimus.platform.dal.allowBackendAccess", false)
  private val logDalResultWeight = DiagnosticSettings.getBoolProperty("optimus.platform.dal.logDalResultWeight", false)
  private val inTest = DiagnosticSettings.getBoolProperty("optimus.inTest", false)
  private val backendAccessAllowed = !inTest || allowBackendAccess

  val shouldLogDalResultWeight: Boolean = inTest && logDalResultWeight

  /* class IllegalBackendAccessException(msg: String) extends Exception(msg)

  def authorizeBackendAccess(backendName: String): Unit = {
    if (!backendAccessAllowed)
      throw new IllegalBackendAccessException(
        s"Not allowed to access $backendName from a regular test case, please move me to a test-dal-backend source dir")
  }

  // ZkClassNameStore access
  private val allowZkClassNameStoreAccessProperty = "optimus.platform.dal.allowZkClassNameStoreAccess"
  private val allowZkClassNameStoreAccess = new AtomicBoolean(
    DiagnosticSettings.getBoolProperty(allowZkClassNameStoreAccessProperty, false))
  private def zkClassNameStoreAccessAllowed = !inTest || allowZkClassNameStoreAccess.get()

  private val forceAllowNonDevZkAccess = new AtomicBoolean(false)
  private def allowNonDevZkAccess = !inTest || forceAllowNonDevZkAccess.get()

  class IllegalZkClassNameStoreAccessException
      extends Exception(
        "Not allowed to access ZkClassNameStore from a test case, please use TestWithInMemoryClassNameStore")

  class IllegalZkAccessException(env: ServiceEnvironment)
      extends Exception(s"Not allowed to access given ZK from tests: $env") {}

  def setZkClassNameStoreAccess(value: Boolean): Unit = allowZkClassNameStoreAccess.set(value)

  def withForcedAllowedNonDevZkAccess[T](logic: => T): T = {
    val originalNonDevZk = forceAllowNonDevZkAccess.get()

    val attempt = Try {
      forceAllowNonDevZkAccess.set(true)
      logic
    }

    forceAllowNonDevZkAccess.set(originalNonDevZk)
    attempt.get
  }

  /**
   * Only designated tests can access ZK in general (things like functional, backend, performance etc) If tests are
   * allowed to access ZK classname store they should only access DEV ZK classnamestore.
   */
  def authorizeZkClassNameStoreAccess(env: ServiceEnvironment): Unit = {
    if (!zkClassNameStoreAccessAllowed) {
      throw new IllegalZkClassNameStoreAccessException
    }

    if (!allowNonDevZkAccess && env != null && env != ServiceEnvironment.dev) {
      throw new IllegalZkAccessException(env)
    }
  } */
}
