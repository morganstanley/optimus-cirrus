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
// docs-snippet:PIIElementUnpickler
package optimus.platform.pickling
import optimus.datatype.Classification.Client
import optimus.datatype.Classification.Worker
import optimus.datatype.FullName
import optimus.datatype.PIIElement
import optimus.platform.annotations.nodeSync
import optimus.scalacompat.collection._

import scala.collection.compat._
import java.util.concurrent.ConcurrentHashMap
import scala.reflect.runtime.universe._
import scala.reflect.runtime.universe.{Try => _}

object PIIElementUnpickler {
  private val constructorCache = new ConcurrentHashMap[String, MethodMirror]()
  private def getConstructor(tpe: Type): MethodMirror = {
    val className = tpe.typeSymbol.asClass.fullName
    constructorCache.computeIfAbsent(
      className,
      { _ =>
        val constructor = tpe.decl(termNames.CONSTRUCTOR).asMethod
        val mirror = runtimeMirror(getClass.getClassLoader)
        mirror.reflectClass(tpe.typeSymbol.asClass).reflectConstructor(constructor)
      }
    )
  }
  def apply(tpe: Type): Unpickler[PIIElement[_]] = new PIIElementUnpickler(tpe)
}

class PIIElementUnpickler(tpe: Type) extends Unpickler[PIIElement[_]] {
  @nodeSync def unpickle(pickled: Any, is: PickledInputStream): PIIElement[_] = {
    pickled match {
      case null => null
      case value: String =>
        val constructor = PIIElementUnpickler.getConstructor(tpe)
        val instance = constructor(value)
        instance.asInstanceOf[PIIElement[_]]
      case _ => throw new IllegalArgumentException(s"Cannot unpickle PIIElement from $pickled for type $tpe")
    }
  }
}
// docs-snippet:PIIElementUnpickler
