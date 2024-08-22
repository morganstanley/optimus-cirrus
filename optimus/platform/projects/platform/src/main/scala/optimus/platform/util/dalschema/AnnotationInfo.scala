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
package optimus.platform.util.dalschema

import scala.reflect.runtime.universe._

final case class AnnotationInfo(a: Annotation, inheritedFrom: Option[FieldInfo] = None) {
  def shortName: String = a.tree.tpe.typeSymbol.name.decodedName.toString
  def fullName: String = a.tree.toString()
  def parameters: Seq[(String, Any)] = ReflectionHelper.getAnnotationParameters(a)
  def isType(tpe: Type): Boolean = a.tree.tpe =:= tpe
  def asString: String = a.tree.toString
}
