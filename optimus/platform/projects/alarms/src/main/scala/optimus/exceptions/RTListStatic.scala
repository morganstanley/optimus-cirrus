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
package optimus.exceptions

import java.security.InvalidParameterException
import java.time.DateTimeException
import java.time.format.DateTimeParseException

import scala.reflect.internal.MissingRequirementError

trait RTExceptionTrait

final class GenericRTException(msg: String) extends Exception(msg) with RTExceptionTrait
final class UnexpectedStateRTException(msg: String) extends Exception(msg) with RTExceptionTrait

object RTListStatic {
  private val basics: Set[String] = (classOf[RTExceptionTrait] :: classOf[RTExceptionInterface]
    :: classOf[IllegalArgumentException] :: classOf[UnsupportedOperationException]
    :: classOf[InvalidParameterException] :: classOf[IndexOutOfBoundsException]
    :: classOf[StringIndexOutOfBoundsException]
    :: classOf[ArrayIndexOutOfBoundsException] :: classOf[NumberFormatException] :: classOf[ArithmeticException]
    :: classOf[NoSuchMethodError] :: classOf[NoSuchMethodException]
    :: classOf[NullPointerException] :: classOf[NoSuchElementException] :: classOf[NotImplementedError]
    :: classOf[MatchError] :: classOf[DateTimeException] :: classOf[DateTimeParseException]
    :: classOf[ClassNotFoundException] :: classOf[ClassCastException]
    :: Nil).map(_.getCanonicalName).toSet

  private val notAlwaysOnClasspath: Set[String] = ("javax.time.CalendricalException" ::
    "javax.time.calendar.format.CalendricalParseException" ::
    "org.apache.commons.math3.exception.NoBracketingException" ::
    "org.apache.commons.math3.exception.NumberIsTooLargeException" ::
    "org.apache.commons.lang.NotImplementedException" ::
    "org.apache.commons.lang3.NotImplementedException" ::
    Nil).toSet

  // Not so sure about these. Please add comments when augmenting.
  private val dubious: Set[String] =
    (classOf[ScalaReflectionException] :: classOf[MissingRequirementError]
      :: Nil).map(_.getCanonicalName).toSet

  private[optimus] val members = basics ++ notAlwaysOnClasspath ++ dubious
}
