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
package optimus.platform.relational.persistence.protocol

import java.lang.reflect.{Array => _, _}

import scala.collection.mutable

private[persistence] class DictionaryProxy(val methodToValues: mutable.HashMap[Method, Any]) extends InvocationHandler {

  override def invoke(proxy: Object, method: Method, args: Array[Object]): Object = {
    val declaringClass = method.getDeclaringClass
    if (declaringClass == classOf[Object]) {
      method match {
        case DictionaryProxy.hashCodeMethod => Int.box(proxyHashCode(proxy))
        case DictionaryProxy.equalsMethod   => Boolean.box(proxyEquals(proxy, args(0)))
        case DictionaryProxy.toStringMethod => proxyToString(proxy)
        case _                              => throw new IllegalStateException(s"Unexpected method call: $method")
      }
    } else {
      val value =
        methodToValues.get(method).getOrElse(throw new IllegalStateException(s"Unexpected method call: $method"))
      value.asInstanceOf[Object]
    }
  }

  protected def proxyHashCode(proxy: Object): Int = {
    proxyToString(proxy).hashCode()
  }

  protected def proxyEquals(proxy: Object, other: Object): Boolean = {
    if (proxy == other) {
      true
    } else if (other == null || !Proxy.isProxyClass(other.getClass)) {
      false
    } else {
      val otherProxy = Proxy.getInvocationHandler(other).asInstanceOf[DictionaryProxy]
      if (otherProxy == null) {
        false
      } else {
        if (otherProxy.methodToValues.size != methodToValues.size) {
          false
        } else {
          var result = true // result is var because we need assign it when traversing methodToValues.
          val it = otherProxy.methodToValues.iterator
          while (result && it.hasNext) {
            val mV = it.next()
            result = methodToValues.get(mV._1).map(x => DictionaryProxy.objectEquals(mV._2, x)).getOrElse(false)
          }
          result
        }
      }
    }
  }

  protected def proxyToString(proxy: Object): String = {
    val name = proxy.getClass.getInterfaces.map(_.getSimpleName).mkString(" with ")
    s"[Proxy $name {" + methodToValues
      .map(t => (t._1.getName(), t._2))
      .toMap
      .map(t => t._1 + " = " + t._2)
      .mkString(", ") + "}]"
  }
}

private[persistence] object DictionaryProxy {
  lazy val hashCodeMethod: Method = classOf[Object].getMethod("hashCode")
  lazy val equalsMethod: Method = classOf[Object].getMethod("equals", Seq(classOf[Object]): _*)
  lazy val toStringMethod: Method = classOf[Object].getMethod("toString")

  def objectEquals(x: Any, y: Any): Boolean = {
    if (x == null) y == null else x.equals(y)
  }
}
