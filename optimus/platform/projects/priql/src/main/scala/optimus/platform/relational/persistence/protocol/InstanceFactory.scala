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

import DefaultInstanceFactory._
import scala.collection.mutable.HashMap

trait InstanceFactory {
  def createEnum(clazz: Class[_], ordinal: Int): Any
  def createInstance(clazz: Class[_]): Any
}

class DefaultInstanceFactory extends InstanceFactory {

  def createEnum(clazz: Class[_], ordinal: Int): Any = {
    val createFunc = class2newFunc.getOrElse(clazz, null)
    if (createFunc == null) {
      var instance: Any = null
      if (clazz.isEnum) {
        val method = clazz.getMethod("valueOf", Array(classOf[Int]): _*)
        method.setAccessible(true)
        instance = method.invoke(null, Array[AnyRef](Int.box(ordinal)): _*)
        class2newFunc(clazz) = i => method.invoke(null, Array[AnyRef](Int.box(i)): _*)
      } else {
        val method = clazz.getMethod("apply", Array(classOf[Int]): _*)
        method.setAccessible(true)
        val enumObj = clazz.getField("MODULE$").get(clazz)
        instance = method.invoke(enumObj, Array[AnyRef](Int.box(ordinal)): _*)
        class2newFunc(clazz) = i => method.invoke(enumObj, Array[AnyRef](Int.box(i)): _*)
      }
      instance
    } else {
      createFunc(ordinal)
    }
  }

  def createInstance(clazz: Class[_]): Any = {
    val createFunc = class2newFunc.getOrElse(clazz, null)
    if (createFunc == null) {
      val ctors = clazz.getConstructors()
      if (ctors.length > 0) {
        // it's a hack but currently BigDecimal has added checks whether passed constructor parameters are not nulls
        if (clazz == classOf[BigDecimal]) BigDecimal(0)
        else {
          val iter = ctors.iterator
          var instance: Any = null
          while (iter.hasNext && instance == null) {
            val ctor = iter.next()
            ctor.setAccessible(true)
            val params = ctor.getParameterTypes.map(getDefaultValue)
            try {
              instance = ctor.newInstance(params: _*)
              class2newFunc(clazz) = i => ctor.newInstance(params: _*)
            } catch {
              case ex: Exception => ;
            }
          }
          if (instance == null) {
            throw new UnsupportedOperationException(s"Cannot create instance of ${clazz.getName}")
          }
          instance
        }
      } else {
        val obj = clazz.getField("MODULE$").get(clazz)
        class2newFunc(clazz) = i => obj
        obj
      }
    } else {
      createFunc(0)
    }
  }

  protected val class2newFunc = new HashMap[Class[_], Int => Any]
}

private[persistence] object DefaultInstanceFactory {

  def getDefaultValue(clazz: Class[_]): AnyRef = {
    defaultValues.getOrElse(clazz, null)
  }

  val defaultValues = Map[Class[_], AnyRef](
    classOf[String] -> "",
    Integer.TYPE -> Int.box(0),
    classOf[Integer] -> Int.box(0),
    java.lang.Boolean.TYPE -> Boolean.box(false),
    classOf[java.lang.Boolean] -> Boolean.box(false),
    java.lang.Short.TYPE -> Short.box(0),
    classOf[java.lang.Short] -> Short.box(0),
    java.lang.Long.TYPE -> Long.box(0L),
    classOf[java.lang.Long] -> Long.box(0L),
    java.lang.Float.TYPE -> Float.box(0.0f),
    classOf[java.lang.Float] -> Float.box(0.0f),
    java.lang.Double.TYPE -> Double.box(0.0d),
    classOf[java.lang.Double] -> Double.box(0.0d),
    java.lang.Byte.TYPE -> Byte.box(0),
    classOf[java.lang.Byte] -> Byte.box(0),
    Character.TYPE -> Char.box('\u0000'),
    classOf[Character] -> Char.box('\u0000'),
    classOf[BigInt] -> BigInt(0),
    classOf[BigDecimal] -> BigDecimal(0),
    classOf[Option[_]] -> None
  );
}
