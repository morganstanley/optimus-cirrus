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

final case class BiometricInformation[T <: DataSubjectCategory](override val sensitiveValue: String)
    extends PIIElement[T] {
  override def maskedValue = "BiometricInformation PII Element"
}

final case class City[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "City PII Element"
}

final case class Cookie[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "Cookie PII Element"
}

final case class Country[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "Country PII Element"
}

// regex-ignore-line persistent exception for PII Element for Open source
final case class CreditCardNumber[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
// regex-ignore-line persistent exception for PII Element for Open source
  override def maskedValue = "Credit Card Number PII Element"
}

final case class CriminalBackground[T <: DataSubjectCategory](override val sensitiveValue: String)
    extends PIIElement[T] {
  override def maskedValue = "Criminal Background PII Element"
}

final case class CVV[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "CVV PII Element"
}

final case class DateOfBirth[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "Date Of Birth PII Element"
}

final case class DebitCardNumber[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "Debit Card Number PII Element"
}

final case class DeviceFingerprint[T <: DataSubjectCategory](override val sensitiveValue: String)
    extends PIIElement[T] {
  override def maskedValue = "Device Fingerprint PII Element"
}

final case class DeviceId[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "Device ID PII Element"
}

final case class DriverLicense[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "Driver License PII Element"
}

final case class EmailAddress[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "Email Address PII Element"
}

final case class Ethnicity[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "Ethnicity PII Element"
}

final case class FaxNumber[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "Fax Number PII Element"
}

// docs-snippet:FullNamePIIClass
final case class FullName[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "FullName PII Element"
}
// docs-snippet:FullNamePIIClass

final case class FirstName[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "First Name PII Element"
}

final case class Gender[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "Gender PII Element"
}

final case class GeneticInformation[T <: DataSubjectCategory](override val sensitiveValue: String)
    extends PIIElement[T] {
  override def maskedValue = "Genetic Information PII Element"
}

final case class Geolocation[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "Geolocation PII Element"
}

final case class HealthMentalPhysical[T <: DataSubjectCategory](override val sensitiveValue: String)
    extends PIIElement[T] {
  override def maskedValue = "Health: Mental/ Physical PII Element"
}

final case class IpAddress[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "IP Address PII Element"
}

final case class ImagesFullFace[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "Images: Full Face PII Element"
}

final case class LastName[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "Last Name PII Element"
}

final case class MothersMaidenName[T <: DataSubjectCategory](override val sensitiveValue: String)
    extends PIIElement[T] {
  override def maskedValue = "Mother’s Maiden Name PII Element"
}

final case class NationalIdStateId[T <: DataSubjectCategory](override val sensitiveValue: String)
    extends PIIElement[T] {
  override def maskedValue = "National ID/ State ID PII Element"
}

final case class Notes[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "Notes PII Element"
}

final case class Passport[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "Passport PII Element"
}

final case class PassportNumber[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "Passport Number PII Element"
}

final case class Password[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "Password PII Element"
}

final case class PhoneNumber[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "Phone Number PII Element"
}

final case class Pin[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "PIN PII Element"
}

final case class PlaceOfBirth[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "Place of Birth PII Element"
}

final case class PoliticalOpinions[T <: DataSubjectCategory](override val sensitiveValue: String)
    extends PIIElement[T] {
  override def maskedValue = "Political Opinions PII Element"
}

final case class Race[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "Race PII Element"
}

final case class RaceEthnicity[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "Race/ Ethnicity PII Element"
}

final case class ReligionPhilosophicalBeliefs[T <: DataSubjectCategory](override val sensitiveValue: String)
    extends PIIElement[T] {
  override def maskedValue = "Religion/ Philosophical Beliefs PII Element"
}

final case class SalaryCompensation[T <: DataSubjectCategory](override val sensitiveValue: String)
    extends PIIElement[T] {
  override def maskedValue = "Salary/ Compensation PII Element"
}

final case class SecurityQuestionAndAnswer[T <: DataSubjectCategory](override val sensitiveValue: String)
    extends PIIElement[T] {
  override def maskedValue = "Security Question and Answer PII Element"
}

final case class SexualOrientationSexLife[T <: DataSubjectCategory](override val sensitiveValue: String)
    extends PIIElement[T] {
  override def maskedValue = "Sexual Orientation/ Sex Life PII Element"
}

final case class ShortName[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "Short Name PII Element"
}

final case class State[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "State PII Element"
}

final case class StreetAddressPOBox[T <: DataSubjectCategory](override val sensitiveValue: String)
    extends PIIElement[T] {
  override def maskedValue = "Street Address/ PO Box PII Element"
}

final case class TaxID[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "TaxID PII Element"
}

final case class TradeUnionMembership[T <: DataSubjectCategory](override val sensitiveValue: String)
    extends PIIElement[T] {
  override def maskedValue = "Trade Union Membership PII Element"
}

final case class UserID[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "User ID PII Element"
}

final case class ZipPostalCode[T <: DataSubjectCategory](override val sensitiveValue: String) extends PIIElement[T] {
  override def maskedValue = "ZIP/ Postal Code PII Element"
}
