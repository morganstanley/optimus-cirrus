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
package optimus.platform.relational.internal

import optimus.platform.relational.RelationalException
import org.springframework.beans.BeanUtils
import scala.util.Try

object ReflectPropertyUtils {

  def getPropertyValue(memberName: String, clazz: Class[_], item: Any): AnyRef = {
    val getterMethod = Try {
      // scala member getter
      clazz.getMethod(memberName)
    } orElse Try {
      BeanUtils.getPropertyDescriptor(clazz, memberName).getReadMethod
    } orElse Try {
      // this method comes from GroupItem and doesn't have mapped property, such as getKey1AsString(), getRows()...
      val methodName = "get" + memberName.substring(0, 1).toUpperCase + memberName.substring(1)
      clazz.getMethod(methodName)
    } getOrElse {
      throw new RelationalException("Cannot get getter method " + memberName + " of " + clazz)
    }

    getterMethod.setAccessible(true)
    getterMethod.invoke(item)
  }

  def setPropertyValue(memberName: String, clazz: Class[_], item: Any, value: AnyRef): Unit = {
    val setterMethod = Try {
      // scala member setter
      clazz.getMethod(memberName + "_eq$")
    } orElse Try {
      BeanUtils.getPropertyDescriptor(clazz, memberName).getWriteMethod
    } getOrElse {
      throw new RelationalException("Cannot find setter method " + memberName + " of " + clazz)
    }

    setterMethod.invoke(item, Array[AnyRef](value): _*)
  }
}
