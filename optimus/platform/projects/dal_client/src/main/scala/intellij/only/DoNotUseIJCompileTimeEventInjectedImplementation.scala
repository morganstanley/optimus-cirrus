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
package intellij.only

import msjava.base.util.uuid.MSUuid
import optimus.platform.BusinessEvent
import optimus.platform.TransactionTimeContext
import optimus.platform.dal.DALEventInfo
import optimus.platform.pickling.PickledOutputStream
import optimus.platform.storable.BusinessEventReference
import optimus.platform.storable.EventInfo
import optimus.platform.storable.Storable

//noinspection ScalaUnusedSymbol, NotImplementedCode
/*
 * Used by jetfire-pc module for the IntelliJ presentation compiler.
 *
 * Our Entity Plugin in the Scala compiler, injects synthetic functions at compile-time for classes and objects
 * that are annotated with @event.
 *
 * The Presentation Compiler for Scala sources injects this trait as a super (parent) to mimic what the Entity Plugin
 * generates. Consequently, the developer see these methods as legal members they can implement (in some use cases) or
 * invoke through type completion (most use cases).
 *
 * Keep in sync with BusinessEvent.
 */
trait DoNotUseIJCompileTimeEventInjectedImplementation extends Storable {
  type InfoType = EventInfo

  override def $info: InfoType = ??? // abstract in BusinessEvent
  override def $permReference: InfoType#PermRefType = ??? // BusinessEvent
  override def dal$isTemporary: Boolean = ??? // BusinessEvent
  override def pickle(out: PickledOutputStream): Unit = ??? // BusinessEventImpl

  def cmid: Option[MSUuid] = ??? // BusinessEvent
  var dal$eventInfo: DALEventInfo = ??? // BusinessEvent
  var dal$eventRef: BusinessEventReference = ??? // BusinessEvent
  var dal$loadTT: TransactionTimeContext = ??? // BusinessEvent
  def representsSameVersionAs(that: BusinessEvent): Option[Boolean] = ??? // BusinessEvent
}
