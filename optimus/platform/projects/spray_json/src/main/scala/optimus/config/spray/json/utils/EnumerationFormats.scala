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

package optimus.config.spray.json.utils

import optimus.config.spray.json.DeserializationException
import optimus.config.spray.json.JsString
import optimus.config.spray.json.JsValue
import optimus.config.spray.json.RootJsonFormat

import scala.reflect.ClassTag
import scala.reflect.{ classTag => TagOfClass }
import scala.reflect.runtime.{universe => ru}

trait EnumerationFormats {
  implicit def EnumerationFormat[T <: Enumeration: ru.TypeTag]: RootJsonFormat[T#Value] = new RootJsonFormat[T#Value] {
    val enu: T = {
      val tt = ru.typeTag[T].tpe.typeSymbol.asClass.selfType.termSymbol.asModule
      ru.runtimeMirror(getClass.getClassLoader).reflectModule(tt).instance.asInstanceOf[T]
    }
    override def write(obj: T#Value): JsValue = JsString(obj.toString)
    override def read(json: JsValue): T#Value = {
      json match {
        case JsString(txt) => enu.withName(txt)
        case _             => throw DeserializationException(s"Expected a value from enum $enu")
      }
    }
  }

  implicit def JavaEnumerationFormat[T <: Enum[T]: ClassTag]: RootJsonFormat[T] = new RootJsonFormat[T] {
    val enu: ClassTag[T] = TagOfClass[T]
    //
    // As we are looking at java enums that extend the Enum type, we get enum definitions like:
    //
    // public enum ABC {
    //    OBJECT_1("Description of Object1")
    // }
    //
    // using obj.name() will return the "OBJECT_1" field name. We need the value of the field, which is given
    // by obj.toString
    //
    override def write(obj: T): JsValue = JsString(obj.toString)
    override def read(json: JsValue): T = {
      val enumerationClass = enu.runtimeClass.asInstanceOf[Class[T]]
      json match {
        case s: JsString =>
          try {
            Enum.valueOf(enumerationClass, s.value)
          } catch {
            case _: IllegalArgumentException =>
              throw DeserializationException(s"Expected a value from enum $enumerationClass, reveived ${s.value}")
          }
        case _ => throw DeserializationException(s"Expected a JsString value")
      }
    }
  }
}
