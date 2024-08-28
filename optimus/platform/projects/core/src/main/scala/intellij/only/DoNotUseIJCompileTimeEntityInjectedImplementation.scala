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

import msjava.slf4jutils.scalalog.Logger
import optimus.entity.EntityInfo
import optimus.platform.TemporalContext
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityReference
import optimus.platform.storable.StorageInfo

//noinspection ScalaUnusedSymbol, NotImplementedCode
/*
 * Used by jetfire-pc module for the IntelliJ presentation compiler.
 *
 * Our Entity Plugin in the Scala compiler, injects synthetic functions at compile-time for classes and objects
 * that are annotated with @entity.
 *
 * The Presentation Compiler for Scala sources injects this trait as a super (parent) to mimic what the Entity Plugin
 * generates. Consequently, the developer see these methods as legal members they can implement (in some use cases) or
 * invoke through type completion (most use cases).
 *
 * Keep in sync with EntityFlavor.
 */
trait DoNotUseIJCompileTimeEntityInjectedImplementation extends Entity {

  def $isModule: Boolean = ???
  def dal$entityRef_=(entityRef: EntityReference): Unit = ??? // EntityFlavor
  def dal$storageInfo_=(info: StorageInfo): Unit = ??? // EntityFlavor
  override def $info: EntityInfo = ???
  override def $inline: Boolean = ??? // EntityImpl
  override def $permReference: InfoType#PermRefType = ??? // EntityImpl
  override def dal$entityRef: EntityReference = ??? // EntityImpl
  override def dal$isTemporary: Boolean = ??? // EntityImpl
  override def dal$loadContext: TemporalContext = ??? // EntityImpl
  override def dal$storageInfo: StorageInfo = ??? // EntityImpl
  override def dal$temporalContext: TemporalContext = ??? // EntityImpl
  override def equals(other: Any): Boolean = ??? // EntityImpl
  override def hashCode(): Int = ??? // EntityImpl
  override def log: Logger = ??? // EntityImpl

  // DO NOT ADD:
  // override def pickle(out: PickledOutputStream): Unit = ???
}
