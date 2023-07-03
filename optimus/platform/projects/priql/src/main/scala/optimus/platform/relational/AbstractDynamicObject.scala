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
package optimus.platform.relational

import java.lang.reflect.{InvocationHandler, Method}

import optimus.graph.Node
import optimus.platform._
import optimus.platform.relational.asm.ASMDynamicObjectFactory
import optimus.platform.relational.internal.OptimusCoreAPI._
import optimus.platform.relational.internal.RelationalUtils
import optimus.platform.relational.tree.TupleTypeInfo
import optimus.platform.relational.tree.TypeInfo
import org.springframework.util.ReflectionUtils

import scala.collection.mutable

abstract class AbstractDynamicObject extends DynamicObject {
  override lazy val hashCode: Int = getAll.##

  override def equals(that: Any): Boolean = that match {
    case d: DynamicObject =>
      d.hashCode == hashCode && d.getAll == getAll
    case _ => false
  }
}

class TupleDynamicObject(val leftObject: DynamicObject, val rightObject: DynamicObject) extends AbstractDynamicObject {
  def contains(propertyName: String): Boolean = {
    leftObject.contains(propertyName) || rightObject.contains(propertyName)
  }

  def get(propertyName: String): Any = {
    if (leftObject.contains(propertyName))
      leftObject.get(propertyName)
    else
      rightObject.get(propertyName)
  }

  lazy val getAll: Map[String, Any] = rightObject.getAll ++ leftObject.getAll
}

/**
 * This acts as a ScalaBasedDynamicFactoryFactory. It takes a given TypeInfo[T] instance and works out the appropriate
 * shape. It then returns a lambda (T => DynamicObject); that is a factory for DynamicObjects dependent on the
 * predefined shape. This ensures that the Shape is only determined once per relation.
 */
object ScalaBasedDynamicObjectFactory {
  def apply[T](ti: TypeInfo[T]): Either[T => Node[DynamicObject], T => DynamicObject] = {
    ti match {
      case TupleTypeInfo(lt: TypeInfo[lType], rt: TypeInfo[rType]) =>
        (apply(lt), apply(rt)) match {
          case (Right(lf), Right(rf)) =>
            Right((t: T) =>
              t match {
                case (l: lType @unchecked, r: rType @unchecked) => new TupleDynamicObject(lf(l), rf(r))
              })
          case (Left(llf), Left(rlf)) =>
            val lnf = asNode.apply$withNode(llf)
            val rnf = asNode.apply$withNode(rlf)
            Left(liftNode((t: T) =>
              t match {
                case (l: lType @unchecked, r: rType @unchecked) => new TupleDynamicObject(lnf(l), rnf(r))
              }))
          case (Left(llf), Right(rf)) =>
            val lnf = asNode.apply$withNode(llf)
            Left(liftNode((t: T) =>
              t match {
                case (l: lType @unchecked, r: rType @unchecked) => new TupleDynamicObject(lnf(l), rf(r))
              }))
          case (Right(lf), Left(rlf)) =>
            val rnf = asNode.apply$withNode(rlf)
            Left(liftNode((t: T) =>
              t match {
                case (l: lType @unchecked, r: rType @unchecked) => new TupleDynamicObject(lf(l), rnf(r))
              }))
        }
      case _ if ti <:< classOf[DynamicObject] =>
        Right(t => t.asInstanceOf[DynamicObject])
      case _ =>
        ASMDynamicObjectFactory(ti)
    }
  }
}

class MapBasedDynamicObject(val data: Map[String, Any]) extends AbstractDynamicObject with Serializable {
  def contains(propertyName: String): Boolean = data.contains(propertyName)
  def get(propertyName: String): Any =
    data.getOrElse(propertyName, throw new RelationalException(s"Do not have property: $propertyName"))
  def getAll: Map[String, Any] = data
  def copy(fields: (String, Any)*): MapBasedDynamicObject = new MapBasedDynamicObject(data ++ fields)

  def this(data: (String, Any)*) = this(Map(data: _*))
}

class MutableDynamicObject extends AbstractDynamicObject {
  private val data = mutable.Map.empty[String, Any]

  def get(propertyName: String): Any =
    data.getOrElse(propertyName, throw new RelationalException(s"Do not have property: $propertyName"))
  def getAll: Map[String, Any] = data.toMap
  def put(propertyName: String, value: Any) = data.put(propertyName, value)
  def putAll(m: Map[String, Any]) = data ++= m
  def contains(propertyName: String) = data.contains(propertyName)
}

object MutableDynamicObject {
  def apply(d: DynamicObject): MutableDynamicObject = {
    val mo = new MutableDynamicObject
    mo.putAll(d.getAll)
    mo
  }
  def apply(m: Map[String, Any]): MutableDynamicObject = {
    val mo = new MutableDynamicObject
    mo.putAll(m)
    mo
  }
}

/**
 * This is a java reflection proxy class which is used for creating interface based data object.
 */
sealed class DynamicObjectJavaProxy(val proxiedClass: Class[_]) extends AbstractDynamicObject with InvocationHandler {
  private val data = mutable.Map.empty[String, Any]

  def get(propertyName: String): Any = data.getOrElse(propertyName, null)
  def getAll: Map[String, Any] = data.toMap
  def put(propertyName: String, value: Any) = data.put(propertyName, value)
  def putAll(m: Map[String, Any]) = data ++= m
  def contains(propertyName: String) = data.contains(propertyName)

  override def invoke(proxy: AnyRef, method: Method, args: Array[AnyRef]): AnyRef = {
    if (ReflectionUtils.isHashCodeMethod(method)) Int.box(this.data.hashCode())
    else if (ReflectionUtils.isToStringMethod(method)) toString
    else if (ReflectionUtils.isEqualsMethod(method) && args != null && args.size == 1) {
      val that = args(0)
      val res = that match {
        case dynamic: DynamicObjectJavaProxy => this.data.equals(dynamic.data)
        case _                               => false
      }

      Boolean.box(res)
    } else if (method.getName.equals("getAll")) {
      this.getAll
    } else if (RelationalUtils.isGetMethod(method.getName()) && (args == null || args.length == 0)) {
      val fieldName = RelationalUtils.getPropertyName(method.getName())
      RelationalUtils.box(this.get(fieldName))
    }
    // in case JavaDynamicObject.getString("propertyName") is called through non-proxy way it should be converted into
    // get("propertyName") at this point.
    // Otherwise it should be called through proxy.getString("propertyName") so it can be converted into
    // get("propertyName") when PropertyGetExpression.buildRelationElement() is called.
    else if ((RelationalUtils.isGetMethod(method.getName) || method.getName == "get") && (args.length == 1)) {
      args(0) match {
        case property: String => RelationalUtils.box(this.get(property))
        case _ =>
          throw new RelationalException(
            s"Unsupported method call in proxy: ${method.getName}, only parameter whose type is String is allowed")
      }
    } else {
      throw new RelationalException(
        s"Unsupported method call in proxy: ${method.getName}, only property getter is allowed")
    }
  }

  override def toString: String = s"[$proxiedClass, data:$data]"
}

/**
 * This is a DynamicObject wrapper that filters property names from the underlying DynamicObject.
 */
class FilteredDynamicObject(val underlying: DynamicObject, val filter: String => Boolean)
    extends AbstractDynamicObject {
  def get(propertyName: String): Any =
    if (filter(propertyName)) underlying.get(propertyName)
    else throw new RelationalException(s"Do not have property: $propertyName")
  def getAll: Map[String, Any] = underlying.getAll.filter { case (property, _) => filter(property) }
  def contains(propertyName: String) = filter(propertyName) && underlying.contains(propertyName)
}
