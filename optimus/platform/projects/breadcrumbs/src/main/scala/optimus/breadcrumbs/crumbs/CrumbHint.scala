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
package optimus.breadcrumbs.crumbs

sealed trait CrumbHint {
  def hintType: String
  def hintValue: String
  override def toString: String = s"$hintType~$hintValue"
}

object CrumbHints {
  import optimus.breadcrumbs.crumbs.CrumbHint._

  final val LongTerm = Set[CrumbHint](LongTermRetentionHint)
  final val Alert = Set[CrumbHint](AlertableHint)
  final val Diagnostics = Set[CrumbHint](DiagnosticsHint)
  private[breadcrumbs] final val Mock = Set[CrumbHint](MockCrumbHint)
}

private[optimus] object CrumbHint {
  implicit def ordering[A <: CrumbHint]: Ordering[A] = Ordering.by(_.toString)
  private[breadcrumbs] def fromString(prop: String): CrumbHint = prop match {
    case CrumbHint.LongTermRetentionHint.toString => LongTermRetentionHint
    case CrumbHint.AlertableHint.toString         => AlertableHint
    case CrumbHint.DiagnosticsHint.toString       => DiagnosticsHint
    case MockCrumbHint.toString                   => MockCrumbHint
    case _                                        => throw new IllegalArgumentException(s"Invalid CrumbHint: $prop")
  }

  private[breadcrumbs] def setFromString(prop: String): Set[CrumbHint] = {
    prop.split(",").map(CrumbHint.fromString).toSet
  }

  // To route to a Kafka Topic using a hint, entry should look similar to this:
  // { property: "CrumbHint", value: "myHintType~myHintValue", topic: "long" }
  // To route to a Splunk Index using the hint, the -sth config entry should look similar to this (`~` required):
  // 'myHintType~myHintValue: myIndex; foo~bar: fooHintIndex; foo~bar,myHintType~myHintValue: fooAndMyIndex'

  // Use this hint to attempt to send the crumb to the long-term retention splunk index
  private[optimus] case object LongTermRetentionHint extends CrumbHint {
    override def hintType: String = "retention"
    override def hintValue: String = "long"
    override final lazy val toString: String = s"$hintType~$hintValue"
  }

  // Use this hint to route the crumb via a low latency data path dedicated to alerts
  private[optimus] case object AlertableHint extends CrumbHint {
    override def hintType: String = "alert"
    override def hintValue: String = "always"
    override final lazy val toString: String = s"$hintType~$hintValue"
  }

  // Use this hint to send the crumb unconditionally to a physically separate diagnostics partition
  private[optimus] case object DiagnosticsHint extends CrumbHint {
    override def hintType: String = "diag"
    override def hintValue: String = "raw"
    override final lazy val toString: String = s"$hintType~$hintValue"
  }

  // Used strictly for tests until a second valid hint is created (hence restricted to breadcrumbs only)
  // Once a second valid hint is created, this MockCrumbHint can be replaced with the new hint in any tests
  private[breadcrumbs] case object MockCrumbHint extends CrumbHint {
    override def hintType: String = "hint_type"
    override def hintValue: String = "hint_val"
    override final lazy val toString: String = s"$hintType~$hintValue"
  }
}
