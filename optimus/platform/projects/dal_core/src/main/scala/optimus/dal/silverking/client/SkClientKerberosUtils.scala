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
package optimus.dal.silverking.client

import com.ms.silverking.net.security.AuthenticationFailedAction
import com.ms.silverking.net.security.Authenticator
import msjava.slf4jutils.scalalog.Logger
import msjava.slf4jutils.scalalog.getLogger
import optimus.dal.silverking.plugin.SkKerberosPlugin

object SkClientKerberosUtils {
  private[silverking] val skKerberosPluginTemplate =
    new SkKerberosPlugin(
      AuthenticationFailedAction.THROW_NON_RETRYABLE,
      AuthenticationFailedAction.THROW_RETRYABLE,
      AuthenticationFailedAction.THROW_RETRYABLE,
      AuthenticationFailedAction.THROW_RETRYABLE
    )
  val skKerberosReflectionStr: String = skKerberosPluginTemplate.toSKDef

  private val log: Logger = getLogger(this)

  def setEnabled(enable: Boolean): Unit = {
    if (enable) this.enable() else this.disable()
  }

  def enable(): Unit = this.synchronized {
    Authenticator.setAuthenticator(skKerberosReflectionStr)
  }

  def disable(): Unit = this.synchronized {
    System.clearProperty(Authenticator.authImplProperty)
    Authenticator.setAuthenticator(null)
    log.info("Sk-Kerberos is disabled")
  }

  def enabled: Boolean = this.synchronized {
    !Authenticator.isNoopAuthenticator
  }
}
