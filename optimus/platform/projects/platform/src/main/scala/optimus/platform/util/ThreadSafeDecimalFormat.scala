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
package optimus.platform.util

import java.math.RoundingMode
import java.text.DecimalFormat

final case class DecimalFormatBuilder(
    formatPattern: Option[String] = None,
    minimumIntegerDigits: Option[Int] = None,
    maximumIntegerDigits: Option[Int] = None,
    minimumFractionDigits: Option[Int] = None,
    maximumFractionDigits: Option[Int] = None,
    positivePrefix: Option[String] = None,
    positiveSuffix: Option[String] = None,
    negativePrefix: Option[String] = None,
    negativeSuffix: Option[String] = None,
    roundingMode: Option[RoundingMode] = None,
    groupingUsed: Option[Boolean] = None
) {

  def build(): ThreadSafeDecimalFormat = ThreadSafeDecimalFormat.fromBuilder(this)

  def setPattern(n: Option[String]): DecimalFormatBuilder = copy(formatPattern = n)
  def setMinimumIntegerDigits(n: Option[Int]): DecimalFormatBuilder = copy(minimumIntegerDigits = n)
  def setMaximumIntegerDigits(n: Option[Int]): DecimalFormatBuilder = copy(maximumIntegerDigits = n)
  def setMinimumFractionDigits(n: Option[Int]): DecimalFormatBuilder = copy(minimumFractionDigits = n)
  def setMaximumFractionDigits(n: Option[Int]): DecimalFormatBuilder = copy(maximumFractionDigits = n)
  def setPositivePrefix(n: Option[String]): DecimalFormatBuilder = copy(positivePrefix = n)
  def setPositiveSuffix(n: Option[String]): DecimalFormatBuilder = copy(positiveSuffix = n)
  def setNegativePrefix(n: Option[String]): DecimalFormatBuilder = copy(negativePrefix = n)
  def setNegativeSuffix(n: Option[String]): DecimalFormatBuilder = copy(negativeSuffix = n)
  def setRoundingMode(n: Option[RoundingMode]): DecimalFormatBuilder = copy(roundingMode = n)
  def setGroupingUsed(n: Option[Boolean]): DecimalFormatBuilder = copy(groupingUsed = n)

  def setPattern(n: String): DecimalFormatBuilder = setPattern(Some(n))
  def setMinimumIntegerDigits(n: Int): DecimalFormatBuilder = setMinimumIntegerDigits(Some(n))
  def setMaximumIntegerDigits(n: Int): DecimalFormatBuilder = setMaximumIntegerDigits(Some(n))
  def setMinimumFractionDigits(n: Int): DecimalFormatBuilder = setMinimumFractionDigits(Some(n))
  def setMaximumFractionDigits(n: Int): DecimalFormatBuilder = setMaximumFractionDigits(Some(n))
  def setPositivePrefix(n: String): DecimalFormatBuilder = setPositivePrefix(Some(n))
  def setPositiveSuffix(n: String): DecimalFormatBuilder = setPositiveSuffix(Some(n))
  def setNegativePrefix(n: String): DecimalFormatBuilder = setNegativePrefix(Some(n))
  def setNegativeSuffix(n: String): DecimalFormatBuilder = setNegativeSuffix(Some(n))
  def setRoundingMode(n: RoundingMode): DecimalFormatBuilder = setRoundingMode(Some(n))
  def setGroupingUsed(n: Boolean): DecimalFormatBuilder = setGroupingUsed(Some(n))
}

/**
 * * [[DecimalFormat]] is unfortunately not thread safe. This can cause unwanted issues such as notorious
 * NullPointerException at run time. This class provides a wrapper on top of the DecimalFormat to ensure the thread
 * safety of the DecimalFormat.
 */
class ThreadSafeDecimalFormat private (builder: DecimalFormatBuilder) {

  private lazy val decimalFormat = new ThreadLocal[DecimalFormat] {
    override def initialValue(): DecimalFormat = {
      val format = builder.formatPattern.map(p => new DecimalFormat(p)).getOrElse(new DecimalFormat())
      builder.minimumIntegerDigits.foreach(format.setMinimumIntegerDigits)
      builder.maximumIntegerDigits.foreach(format.setMaximumIntegerDigits)
      builder.minimumFractionDigits.foreach(format.setMinimumFractionDigits)
      builder.maximumFractionDigits.foreach(format.setMaximumFractionDigits)
      builder.positivePrefix.foreach(format.setPositivePrefix)
      builder.positiveSuffix.foreach(format.setPositiveSuffix)
      builder.negativePrefix.foreach(format.setNegativePrefix)
      builder.negativeSuffix.foreach(format.setNegativeSuffix)
      builder.roundingMode.foreach(format.setRoundingMode)
      builder.groupingUsed.foreach(format.setGroupingUsed)
      format
    }
  }

  def format(number: BigDecimal): String = decimalFormat.get().format(number)
  def format(number: Int): String = decimalFormat.get.format(number)
  def format(number: Double): String = decimalFormat.get.format(number)
  def format(number: Long): String = decimalFormat.get.format(number)
  def parse(s: String): Number = decimalFormat.get.parse(s)

}

object ThreadSafeDecimalFormat {
  def fromBuilder(builder: DecimalFormatBuilder) = new ThreadSafeDecimalFormat(builder)
  def pattern(pattern: String): ThreadSafeDecimalFormat =
    new ThreadSafeDecimalFormat(DecimalFormatBuilder(formatPattern = Some(pattern)))
}
