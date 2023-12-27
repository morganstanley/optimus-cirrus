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
package optimus.datatype
import optimus.datatype.Classification.DataSubjectCategory

/**
 * This trait is indicator for PIIElement
 */
sealed trait PIIElement {
  val sensitiveValue: String
  def maskedValue = "PII Element"
}

/**
 * This trait is indicator if element is 100 % confirmed that it is NotAPIIElement
 */
sealed trait NotAPIIElement {}

object Classification {
  trait DataSubjectCategory
  class Worker extends DataSubjectCategory
  class Client extends DataSubjectCategory
  class ThirdParty extends DataSubjectCategory
}

final case class FullName[T <: DataSubjectCategory](val sensitiveValue: String) extends PIIElement {
  override def equals(other: Any): Boolean =
    other match {
      case t: FullName[_] => t.sensitiveValue == sensitiveValue
      case _              => false
    }
  override def hashCode() = sensitiveValue.hashCode
  override def maskedValue = "FullName PII Element"
  override def toString = s"FullName PII Element($hashCode)"
}
object FullName {
  def apply[T <: DataSubjectCategory](sensitiveValue: String): FullName[T] = new FullName[T](sensitiveValue)
}
