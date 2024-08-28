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
package optimus.graph.diagnostics

import java.util.prefs.Preferences
import optimus.graph.DiagnosticSettings

import scala.collection.mutable.ArrayBuffer

class DbgPreference(val name: String, val title: String, val tooltip: String, pref: Preferences, initValue: Boolean) {
  def this(name: String) = this(name, null, null, DbgPreference.defaultPref, false)
  def this(name: String, title: String, tooltip: String) = this(name, title, tooltip, DbgPreference.defaultPref, false)
  private val nameLC = name.toLowerCase // Avoid strange encoding by java pref
  private var value: Boolean = pref.getBoolean(nameLC, initValue)
  def set(newValue: Boolean): Unit = {
    value = newValue
    pref.putBoolean(nameLC, newValue)
    onValueChanged()
  }

  // Only read from preferences if a UI is up (either regular console or 'offline' TraceReloaded), otherwise don't, as
  // some preferences will affect test runs, e.g. suspendOnTestFailed
  def get: Boolean = (DiagnosticSettings.diag_showConsole || DiagnosticSettings.offlineReview) && value

  def ?[T](ifv: => T) = new otherwise(ifv _)
  final class otherwise[T] private[DbgPreference] (ifv: () => T) {
    def |(elsev: => T): T = if (get) ifv() else elsev
  }

  protected def onValueChanged(): Unit = {}
}

/** Notifies registered caller and provides value of enabled/disabled flag to callback */
class NotifyingDbgPreference(override val name: String) extends DbgPreference(name) {
  private val callbacks = ArrayBuffer[Boolean => Unit]()

  def addCallback(f: Boolean => Unit): Unit = callbacks.synchronized { callbacks += f }

  override protected def onValueChanged(): Unit = callbacks.foreach(_(get))
}

object DbgPreference {
  private val defaultPref = Preferences.userNodeForPackage(classOf[DbgPreference])

  def apply(name: String, pref: Preferences): DbgPreference =
    new DbgPreference(name, null, null, pref, false)

  def apply(name: String, default: Boolean): DbgPreference =
    new DbgPreference(name, null, null, defaultPref, default)

  def apply(name: String, title: String, tooltip: String, default: Boolean): DbgPreference =
    new DbgPreference(name, title, tooltip, defaultPref, default)

  def apply(name: String, title: String, tooltip: String, pref: Preferences): DbgPreference =
    new DbgPreference(name, title, tooltip, pref, false)
}
