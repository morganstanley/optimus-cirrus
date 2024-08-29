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
package optimus.platform.dal

import java.time.Instant
import optimus.dsi.session.EstablishedClientSession
import optimus.graph.DiagnosticSettings
import optimus.platform._
import optimus.platform.dal._
import optimus.platform.EvaluationContext
import optimus.platform.dsi.bitemporal.DSI

object SessionFetcher {
  private val enableGlobalSessionTtProp = "optimus.platform.dal.enableGlobalSessionTt"
  private[optimus] val enableGlobalSessionTt = DiagnosticSettings.getBoolProperty(enableGlobalSessionTtProp, true)

  private[optimus] def forceCurrentGlobalSessionTxTimeTo(tt: Instant): Boolean = {
    if (EvaluationContext.isInitialised && EvaluationContext.entityResolver.isInstanceOf[SessionFetcher]) {
      EvaluationContext.entityResolver.asInstanceOf[SessionFetcher].forceGlobalSessionEstablishmentTimeTo(tt)
      true
    } else {
      false
    }
  }
}

trait SessionFetcher { self: EntityResolver =>
  protected[optimus] def dsi: DSI
  @async private[optimus] def getSession(tryEstablish: Boolean = true): EstablishedClientSession

  private[optimus] def initGlobalSessionEstablishmentTime(time: Instant): Unit
  private[optimus] def getGlobalSessionEstablishmentTime: Option[Instant]
  private[optimus] def forceGlobalSessionEstablishmentTimeTo(time: Instant): Option[Instant]
}
