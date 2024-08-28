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
package optimus.platform.internal

import java.util.concurrent.ConcurrentHashMap
import optimus.platform.storable.Entity

private[optimus] sealed trait ClassInfo
object KnownClassInfo {
  def unapply[T <: Entity](info: KnownClassInfo[T]) = Some(info.clazz, info.isClazzConcrete)
}
object ConcreteClassInfo {
  private[this] val knownConcreteCache = new ConcurrentHashMap[Class[_ <: Entity], ConcreteClassInfo[_]]()
  def apply[T <: Entity](clazz: Class[T]): ConcreteClassInfo[T] = {
    val orig = knownConcreteCache.get(clazz)
    val res =
      if (orig ne null) orig
      else {
        val replace = new ConcreteClassInfo(clazz)
        val updated = knownConcreteCache.putIfAbsent(clazz, replace)
        if (updated eq null) replace else updated
      }
    res.asInstanceOf[ConcreteClassInfo[T]]
  }
}
object SpecificationClassInfo {
  private[this] val knownSpecificationCache = new ConcurrentHashMap[Class[_ <: Entity], SpecificationClassInfo[_]]()
  def apply[T <: Entity](clazz: Class[T]): SpecificationClassInfo[T] = {
    val orig = knownSpecificationCache.get(clazz)
    val res =
      if (orig ne null) orig
      else {
        val replace = new SpecificationClassInfo(clazz)
        val updated = knownSpecificationCache.putIfAbsent(clazz, replace)
        if (updated eq null) replace else updated
      }
    res.asInstanceOf[SpecificationClassInfo[T]]
  }
}
private[optimus] sealed trait KnownClassInfo[T <: Entity] extends ClassInfo {
  def clazz: Class[T]
  def isClazzConcrete: Boolean
  override def toString: String = s"${this.getClass}($clazz, isConcrete: $isClazzConcrete)"
}
private[optimus] final class ConcreteClassInfo[T <: Entity] private (val clazz: Class[T]) extends KnownClassInfo[T] {
  def isClazzConcrete = true
}
private[optimus] final class SpecificationClassInfo[T <: Entity] private (val clazz: Class[T])
    extends KnownClassInfo[T] {
  def isClazzConcrete = false
}
private[optimus] sealed trait UnknownClassInfo extends ClassInfo {
  def description: String
  override def toString: String = description
}
private[optimus] final case class NoClassInfo(description: String) extends UnknownClassInfo
private[optimus] case object UnknownEntityReference extends UnknownClassInfo {
  def description = "unknown entity reference"
}
