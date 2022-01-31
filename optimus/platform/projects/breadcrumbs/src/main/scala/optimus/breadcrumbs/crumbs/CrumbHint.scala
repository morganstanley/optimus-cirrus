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
  private[breadcrumbs] final val Mock = Set[CrumbHint](MockCrumbHint)
}

private[optimus] object CrumbHint {
  implicit def ordering[A <: CrumbHint]: Ordering[A] = Ordering.by(_.toString)
  private[breadcrumbs] def fromString(prop: String): CrumbHint = prop match {
    case CrumbHint.LongTermRetentionHint.toString => LongTermRetentionHint
    case MockCrumbHint.toString                   => MockCrumbHint
    case _                                        => throw new IllegalArgumentException(s"Invalid CrumbHint: $prop")
  }

  private[breadcrumbs] def setFromString(prop: String): Set[CrumbHint] = {
    prop.split(",").map(CrumbHint.fromString).toSet
  }

  // Use this hint to attempt to send the crumb to the long-term retention splunk index
  // To route to a Kafka Topic using the hint, entry should look similar to this:
  // { property: "CrumbHint", value: "retention~long", topic: "long" }
  // To route to a Splunk Index using the hint, the -sth config entry should look similar to this (`~` required):
  // 'retention~long: long; foo~bar: fooHintIndex; foo~bar,retention~long: fooAndLongIndex'
  private[optimus] case object LongTermRetentionHint extends CrumbHint {
    override def hintType: String = "retention"
    override def hintValue: String = "long"
    override final lazy val toString: String = s"$hintType~$hintValue"
  }

  // Use this hint to route the crumb via a low latency data path dedicated to alerts
  // To route to a Kafka Topic using the hint, entry should look similar to this:
  // { property: "CrumbHint", value: "retention~long", topic: "long" }
  // To route to a Splunk Index using the hint, the -sth config entry should look similar to this (`~` required):
  // 'alert~always: long; foo~bar: fooHintIndex; foo~bar,alert~always: fooAndAlertIndex'
  private[optimus] case object AlertableHint extends CrumbHint {
    override def hintType: String = "alert"
    override def hintValue: String = "always"
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
