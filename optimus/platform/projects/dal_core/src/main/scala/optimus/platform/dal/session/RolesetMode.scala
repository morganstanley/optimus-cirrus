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
package optimus.platform.dal.session

import scala.collection.immutable.SortedSet

/*
 * Supertype specifying a roleset to be used in establishing a session
 */
sealed trait RolesetMode extends Any

object RolesetMode {
  /*
   * Supertype specifying that a RolesetMode is valid in established sessions
   */
  sealed trait Established extends Any with RolesetMode { self: RolesetMode => }

  /*
   * Use a specific (provided) roleset to establish a session
   */
  final case class SpecificRoleset private (underlying: SortedSet[String])
      extends AnyVal
      with RolesetMode
      with Established {
    override def toString: String = s"SpecificRoleset(${underlying.mkString(",")})"
    def prettyString: String = underlying.mkString("[", ",", "]")
  }
  object SpecificRoleset {
    def commaSeparated(rolesetString: String): SpecificRoleset =
      mustBeNonEmpty(rolesetString.split(",").map(_.trim).filter(_.nonEmpty).toSet :: Nil)
    def mustBeNonEmpty(rolesets: Seq[Set[String]]): SpecificRoleset = {
      require(rolesets.nonEmpty, "rolesets must be non-empty")
      apply(rolesets.flatten.toSet)
    }
    def apply(rs: Set[String]): SpecificRoleset = {
      apply(SortedSet.empty[String] ++ rs)
    }
  }

  /*
   * Establish a session with all available roles
   */
  case object AllRoles extends RolesetMode {
    override def toString: String = "AllRoles"
  }

  /*
   * Don't use a roleset at all, use legacy entitlements instead
   */
  case object UseLegacyEntitlements extends RolesetMode with Established {
    override def toString: String = "LegacyEntitlements"
  }

  /*
   * By default we establish with an empty roleset, this means we will use role-based entitlements but only want the
   * roles the broker gives us automagically (the default role)
   */
  private[optimus] val Default: RolesetMode = fromSeqSetString(Seq(Set.empty))

  // only left here for compatibility, should just use a RolesetMode constructor directly.
  def fromSeqSetString(seqSetString: Seq[Set[String]]): RolesetMode = {
    if (seqSetString.isEmpty) RolesetMode.UseLegacyEntitlements
    else RolesetMode.SpecificRoleset.mustBeNonEmpty(seqSetString)
  }
}
