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

trait FieldPiiClassification[T <: DataSubjectCategory] {}

/**
 * This trait is indicator for PIIElement
 */
// docs-snippet:PiiElementTrait
sealed trait PIIElement[T <: DataSubjectCategory] {
  val sensitiveValue: String
  def maskedValue = "PII Element"
  override def equals(other: Any): Boolean =
    other match {
      case t: PIIElement[_] => t.sensitiveValue == sensitiveValue && t.getClass == getClass
      case _                => false
    }

  override def hashCode() = sensitiveValue.hashCode
  override def toString = s"$maskedValue($hashCode)"
}
// docs-snippet:PiiElementTrait
/**
 * This trait is indicator if element is 100 % confirmed that it is NotAPIIElement
 */
// docs-snippet:NotAPIITrait
trait NotAPIIElement {}
// docs-snippet:NotAPIITrait
// docs-snippet:DataSubjectCategory
object Classification {
  trait DataSubjectCategory
  class Worker extends DataSubjectCategory
  class Client extends DataSubjectCategory
  class ThirdParty extends DataSubjectCategory
}
// docs-snippet:DataSubjectCategory

final case class AccountNumber[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "AccountNumber PII Element"
}

final case class City[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "City PII Element"
}

final case class Country[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "Country PII Element"
}

final case class EmailAddress[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "Email Address PII Element"
}

// docs-snippet:FullNamePIIClass
final case class FullName[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "FullName PII Element"
}
// docs-snippet:FullNamePIIClass

final case class IpAddress[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "IP Address PII Element"
}

final case class UserID[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "User ID PII Element"
}

final case class ZipPostalCode[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "ZIP/ Postal Code PII Element"
}

// docs-snippet:FieldPIIClassification
object PII {
  final case class AccountNumber[T <: DataSubjectCategory]() extends FieldPiiClassification[T]
  final case class City[T <: DataSubjectCategory]() extends FieldPiiClassification[T]
  final case class Country[T <: DataSubjectCategory]() extends FieldPiiClassification[T]
  final case class EmailAddress[T <: DataSubjectCategory]() extends FieldPiiClassification[T]
  final case class FullName[T <: DataSubjectCategory]() extends FieldPiiClassification[T]
  final case class IpAddress[T <: DataSubjectCategory]() extends FieldPiiClassification[T]
  final case class UserID[T <: DataSubjectCategory]() extends FieldPiiClassification[T]
  final case class ZipPostalCode[T <: DataSubjectCategory]() extends FieldPiiClassification[T]
}
// docs-snippet:FieldPIIClassification
