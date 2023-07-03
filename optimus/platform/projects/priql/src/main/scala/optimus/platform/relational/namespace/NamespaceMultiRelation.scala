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
package optimus.platform.relational.namespace

import optimus.platform.relational.tree._
import optimus.platform._
import optimus.platform.relational.persistence.protocol.ProviderPersistence
import optimus.platform.relational.persistence.protocol.ProviderPersistenceSupport
import optimus.platform.relational.persistence.protocol._

import scala.collection.mutable.ListBuffer

final class Namespace[T] private (val namespace: String, val includesSubPackage: Boolean) extends Serializable {
  override def toString = s"Namespace( $namespace, includesSubPackage = $includesSubPackage )"
  override def equals(obj: scala.Any): Boolean = obj match {
    case ns: Namespace[_] => this.namespace == ns.namespace && this.includesSubPackage == ns.includesSubPackage
    case _                => false
  }
  override def hashCode(): Int = namespace.hashCode ^ includesSubPackage.hashCode()
}

object Namespace {
  // Creates a NamespaceSource instance which encapsulates a namespace and is stored on the intermediate tree.
  // This will be used for permissioning etc.
  def apply(namespace: String, includesSubPackage: Boolean = false) = new Namespace[Any](namespace, includesSubPackage)
  def apply(clazz: Class[_]): Namespace[Any] = apply(clazz, false)
  def apply(clazz: Class[_], includesSubPackage: Boolean) =
    new Namespace[Any](clazz.getPackage.getName, includesSubPackage)
  def apply[T: reflect.ClassTag]: Namespace[Any] = apply[T](false)
  def apply[T: reflect.ClassTag](includesSubPackage: Boolean) =
    new Namespace[Any](reflect.classTag[T].runtimeClass.getPackage.getName, includesSubPackage)
}

class NamespaceMultiRelation[T](
    val source: Namespace[T],
    typeInfo: TypeInfo[T],
    key: RelationKey[_],
    pos: MethodPosition)
    extends ProviderRelation(typeInfo, key, pos)
    with ProviderPersistenceSupport {
  override def getProviderName = "NamespaceProvider"

  override def prettyPrint(indent: Int, out: StringBuilder): Unit = {
    indentAndStar(indent, out)
    out ++= serial + " Provider:" + " '" + getProviderName + "' " + "of" + " type " + typeInfo.name + " with namespace " + source.namespace + "(sub = " + source.includesSubPackage + ")\n"
  }

  override def fillQueryExplainItem(level_id: Integer, table: ListBuffer[QueryExplainItem]): Unit = {
    table += new QueryExplainItem(
      level_id,
      getProviderName,
      s"${projectedType().name}",
      s"with namespace ${source.namespace} (sub = ${source.includesSubPackage})",
      0)
  }

  override def serializerForThisProvider: ProviderPersistence = NamespaceProviderPersistence

  override def makeKey(newKey: RelationKey[_]) = new NamespaceMultiRelation[T](source, typeInfo, newKey, pos)
}

object NamespaceMultiRelation {
  def apply[T: TypeInfo](source: Namespace[T], key: RelationKey[_], pos: MethodPosition): NamespaceMultiRelation[T] = {
    new NamespaceMultiRelation[T](source, typeInfo[T], key, pos)
  }

  def unapply(namespaceMultiRelation: NamespaceMultiRelation[_]) =
    Some(namespaceMultiRelation.source.namespace, namespaceMultiRelation.source.includesSubPackage)
}
