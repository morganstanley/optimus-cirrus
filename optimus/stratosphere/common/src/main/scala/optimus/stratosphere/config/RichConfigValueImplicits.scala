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
package optimus.stratosphere.config

import java.time.LocalDate
import scala.util.matching.Regex

trait RichConfigValueImplicits {

  trait RichConfigValuePprint {

    protected def configValue: RichConfigValue[_]

    protected def title(configSource: RichConfigValue[_]): String =
      configSource.displayName.getOrElse(configSource.key)
    def pprint(title: String): Option[String]
    final def pprint: Option[String] = pprint(title(configValue))
  }

  implicit class RichConfigValueSeqHelper[T](richConfigValue: RichConfigValue[Seq[T]])(implicit
      consoleColors: ConsoleColors)
      extends RichConfigValuePprint {
    override protected def configValue: RichConfigValue[_] = richConfigValue
    override def pprint(title: String): Option[String] = {
      Option.when(richConfigValue.value.nonEmpty) {
        s"$title: ${richConfigValue.location.pprint(richConfigValue.value.mkString(", "))}"
      }
    }
  }

  implicit class RichConfigValueOptionHelper[T](richConfigValue: RichConfigValue[Option[T]])(implicit
      consoleColors: ConsoleColors)
      extends RichConfigValuePprint {
    override protected def configValue: RichConfigValue[_] = richConfigValue
    override def pprint(title: String): Option[String] =
      richConfigValue.value.map(value => s"$title: ${richConfigValue.location.pprint(value.toString)}")
  }

  implicit class RichConfigValueBaseHelper[T](richConfigValue: RichConfigValue[T])(implicit
      consoleColors: ConsoleColors)
      extends RichConfigValuePprint {
    override protected def configValue: RichConfigValue[_] = richConfigValue
    override def pprint(title: String): Option[String] =
      Some(s"$title: ${richConfigValue.location.pprint(richConfigValue.value.toString)}")
  }

  implicit class RichConfigValueHelper(value: RichConfigValue[String]) {
    def withReleaseDate(dateFormat: Regex): RichConfigValue[String] = {
      val withReleaseDate: Option[LocalDate] =
        value.value match {
          case dateFormat(year, month, day) => Some(LocalDate.of(year.toInt, month.toInt, day.toInt))
          case _                            => None
        }
      RichConfigValue(
        value.key,
        value.value,
        value.location,
        value.displayName,
        releaseDate = withReleaseDate,
        environmentKeyName = value.environmentKeyName)
    }
  }

}
