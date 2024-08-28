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
package optimus.platform

import scala.annotation.nowarn
import msjava.slf4jutils.scalalog._
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.CopyOnWriteArrayList

import optimus.platform.annotations.deprecating

import scala.jdk.CollectionConverters._

trait DALStateEvent

trait DALRetryEvent extends DALStateEvent {
  def exception: Exception
}

final case class DALClientRetryEvent(val exception: Exception) extends DALRetryEvent
final case class DALLeaderRetryEvent(val exception: Exception, val broker: String) extends DALRetryEvent
final case class DALReadOnlyRetryEvent(val exception: Exception, val broker: String) extends DALRetryEvent
final case class DALRecoveryEvent(val message: String) extends DALStateEvent

final case class DALVersioningRetryEvent(val exception: Exception) extends DALRetryEvent
final case class DALVersioningRecoveryEvent(val message: String) extends DALStateEvent

final case class DALFatalErrorEvent(val exception: Throwable) extends DALStateEvent
final case class DALTimeoutEvent(val timeoutMilliSeconds: Int) extends DALStateEvent

/**
 * Publish DAL Event to all @{link DALStateListener}s
 */
object DALEventMulticaster {

  private val log = getLogger[DALRetryEvent]

  /**
   * DALStateListener to get all @{link DALStateEvent}s from DAL API. TODO
   * (OPTIMUS-15409): Remove this once all existing usages move away from this.
   */
  @deprecating(
    "DO NOT USE. This is going to be removed in favor of cancellation scope and NodeTry/asyncResult features" +
      "in Optimus. Instead, corresponding fatal and timeout exceptions will be thrown, which then should be handled by clients..")
  trait DALStateListenerDoNotUse {
    def retryCallback(e: DALRetryEvent): Unit = {}
    def recoveryCallback(e: DALRecoveryEvent): Unit = {}
    def versioningRetryCallback(e: DALVersioningRetryEvent): Unit = {}
    def versioningRecoveryCallback(e: DALVersioningRecoveryEvent): Unit = {}
    def fatalDALErrorCallback(e: DALFatalErrorEvent): Unit = {}
    def timeoutCallback(e: DALTimeoutEvent): Unit = {}
  }

  @nowarn("msg=10500 optimus.platform.DALEventMulticaster.DALStateListenerDoNotUse")
  private val listeners = new CopyOnWriteArrayList[DALStateListenerDoNotUse]()

  @nowarn("msg=10500 optimus.platform.DALEventMulticaster.DALStateListenerDoNotUse")
  def getListeners: List[DALStateListenerDoNotUse] = listeners.iterator().asScala.toList
  @nowarn("msg=10500 optimus.platform.DALEventMulticaster.DALStateListenerDoNotUse")
  def addListener(listener: DALStateListenerDoNotUse): Unit = listeners.add(listener)
  @nowarn("msg=10500 optimus.platform.DALEventMulticaster.DALStateListenerDoNotUse")
  def removeListener(listener: DALStateListenerDoNotUse): Unit = listeners.remove(listener)

  private[optimus] def dalRetryCallback(e: DALRetryEvent): Unit = notifyListeners(_.retryCallback(e))
  private[optimus] def dalRecoveryCallback(e: DALRecoveryEvent): Unit = notifyListeners(_.recoveryCallback(e))
  private[optimus] def dalVersioningRetryCallback(e: DALVersioningRetryEvent): Unit =
    notifyListeners(_.versioningRetryCallback(e))
  private[optimus] def dalVersioningRecoveryCallback(e: DALVersioningRecoveryEvent): Unit =
    notifyListeners(_.versioningRecoveryCallback(e))

  @nowarn("msg=10500 optimus.platform.DALEventMulticaster.DALStateListenerDoNotUse")
  private[optimus] def notifyListeners(doCallback: DALStateListenerDoNotUse => Unit): Unit = {
    callbackExecutor.execute(new Runnable() {
      def run: Unit =
        listeners.asScala.foreach(l => {
          try {
            doCallback(l)
          } catch {
            case e: Exception => log.warn("Error in DALStateListener callback. ", e)
          }
        })
    })
  }

  private[optimus] val callbackExecutor = Executors.newFixedThreadPool(
    1,
    new ThreadFactory {
      override def newThread(r: Runnable) = {
        val t = new Thread(r, "DAL Listener callback thread")
        t.setDaemon(true)
        t
      }
    })
}
