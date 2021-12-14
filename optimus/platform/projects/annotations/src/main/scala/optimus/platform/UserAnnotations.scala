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
package optimus.platform

import scala.annotation.meta._
import scala.annotation._

/**
 * Marks a def/val as overridable
 * NOTE: Tweak modifier is recognized by the compiler plugin
 * To change its default value it's not enough to change to tweak = false. You also have to change EntitySettings.tweakByDefault in the compiler plugin
 * @see http://optimusdoc/BasicAnnotations
 */
@getter
class node(tweak: Boolean) extends StaticAnnotation {
  def this() = this(false)
}

/**
 * @see http://optimusdoc/BasicAnnotations
 */
@getter
class scenarioIndependent extends StaticAnnotation

/**
 * Mark an @node def to be evaluated (and then cached) using the initial runtime environment.
 * @see http://optimusdoc/BasicAnnotations
 */
class givenRuntimeEnv extends Annotation

/**
 * Mark an @async val to be evaluated (and then stored in object memory) using an initial runtime environment.
 * @see http://optimusdoc/BasicAnnotations
 */
class givenAnyRuntimeEnv extends Annotation

/**
 * Marks def as a legal entry point into the graph and turns off warning of
 * using async function from sync context
 * @see http://optimusdoc/InterAnnotations
 */
class entersGraph extends StaticAnnotation

/**
 * Turns off some optimizations
 * @see http://optimusdoc/InterAnnotations
 */
class asyncOff extends Annotation

/**
 * Mark the annotated class an entity.
 * Or the annotated trait for use as an entity interface or mixin.
 *
 * This annotation is processed at compile time by the Optimus 'entity' scalac plugin.
 * Used lower-case name to avoid collision between annotation and regular class names (esp for Entity).
 *
 * An @stored @entity class is rewritten to extend an Entity base, and Entity member annotations will be processed.
 *
 * @see http://optimusdoc/BasicAnnotations
 */
class entity(schemaVersion: Int /* = 0 */ ) extends StaticAnnotation { // Defaults filled in via compiler plugin.  Not specifying here since it helps fail-fast when it's not in use.
  /** This constructor is removed by the staging plugin and exists only to improve the IntelliJ presentation compiler experience */
  @staged def this(schemaVersion: Int = 0, fakeParamForIntellij: Boolean = true) =
    this(schemaVersion)
}

/**
 * @see http://optimusdoc/BasicAnnotations
 */
@getter
class key extends StaticAnnotation

/**
 * @see http://optimusdoc/BasicAnnotations
 */
@getter
class indexed(
    val unique: Boolean /* = false */,
    val queryable: Boolean /* = true */,
    val queryByEref: Boolean /* = false*/ )
    extends StaticAnnotation { // Defaults filled in via compiler plugin.
  /** This constructor is removed by the staging plugin and exists only to improve the IntelliJ presentation compiler experience */
  @staged def this(
      unique: Boolean = false,
      queryable: Boolean = false,
      queryByEref: Boolean = false,
      fakeParamForIntellij: Boolean = true) =
    this(unique, queryable, queryByEref)
}

/**
 * @see http://optimusdoc/BasicAnnotations
 */
@field
class stored(val childToParent: Boolean = false, val projected: Boolean = false) extends StaticAnnotation

/**
 * @see http://optimusdoc/InterAnnotations
 */
@field
class backed extends Annotation

/**
 * @see http://optimusdoc/BasicAnnotations
 */
class embeddable(projected: Boolean = false) extends StaticAnnotation

/**
 * applied to case classes
 * A @stable case class has a cached eagerly generated hashcode
 * and an equals method that makes use of the hashcode
 *
 * Stable case classes use optimus equality rules (e.g. for NaN)
 * @embeddable implies @stable
 */
class stable extends StaticAnnotation

/**
 * applies to @stable/@embeddable constructor parameters -
 * exclude this parameter from equals/hashcode calculations
 */
class notPartOfIdentity extends StaticAnnotation

/**
 * @see http://optimusdoc/BasicAnnotations
 */
class event(projected: Boolean = false) extends StaticAnnotation

/**
 * This annotation explicitly excludes one constructor parameter from the copy method we generate,
 * and can make a concrete type extend concrete type mask values in the constructor
 * @see http://optimusdoc/InterAnnotations
 */
class copyAsIs extends StaticAnnotation

/**
 * @see http://optimusdoc/InterAnnotations
 */
class impure extends StaticAnnotation

// Mark methods that really should be impure, but it would break too much code to mark them so right now.
private[optimus] class aspirationallyimpure extends StaticAnnotation

sealed trait UpcastDomain {
  val id: String
}

case object UpcastingUtil {
  def isInvalidDomain(domain: String): Boolean = {
    domain.toLowerCase() match {
      case CMUpcastDomain.id => false
      case _                 => true
    }
  }
}

/**
 * CM domain for upcasting, upcasting needs to be enabled for this domain in order for
 * any entity to be upcasted to an entity annotated with @upcastingTarget(CMUpcastDomain)
 */
case object CMUpcastDomain extends UpcastDomain {
  override val id = "cm"
}

/**
 * mark the annotated @stored @entity class as a target for upcasting
 * if the given domain is enabled
 * @see http://optimusdoc/InterAnnotations
 */
sealed class upcastingTarget(val domain: UpcastDomain) extends StaticAnnotation

/**
 * During compilation, write diagnostics about graph nodes enclosed by this annotation to the given directory.
 */
case class nodeDebug(dir: String)

// You don't want to use this - the intellij plugin injects it to help with highlighting, but it
// doesn't actually get compiled, and if it did it wouldn't do anything.
class ofInterestInIDE(val reason: String = "optimus") extends StaticAnnotation