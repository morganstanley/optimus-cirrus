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
 * @see
 *   http://optimusdoc/BasicAnnotations
 */
@getter
class siRhs extends StaticAnnotation

/**
 * Mark an @node def to be evaluated (and then cached) using the initial runtime environment.
 * @see
 *   http://optimusdoc/BasicAnnotations
 */
class givenRuntimeEnv extends Annotation

/**
 * Mark an @async val to be evaluated (and then stored in object memory) using an initial runtime environment.
 * @see
 *   http://optimusdoc/BasicAnnotations
 */
class givenAnyRuntimeEnv extends Annotation

/**
 * Marks def as a legal entry point into the graph and turns off warning of using async function from sync context
 * @see
 *   http://optimusdoc/InterAnnotations
 */
class entersGraph extends StaticAnnotation

/**
 * Turns off some optimizations
 * @see
 *   http://optimusdoc/InterAnnotations
 */
class asyncOff extends Annotation

/**
 * @see
 *   http://optimusdoc/BasicAnnotations
 */
@getter
class key extends StaticAnnotation

/**
 * @see
 *   http://optimusdoc/InterAnnotations
 */
@field
class backed extends Annotation

/**
 * applied to case classes A @stable case class has a cached eagerly generated hashcode and an equals method that makes
 * use of the hashcode
 *
 * Stable case classes use optimus equality rules (e.g. for NaN)
 * @embeddable
 *   implies @stable
 */
class stable extends StaticAnnotation

/**
 * applies to @stable/@embeddable constructor parameters - exclude this parameter from equals/hashcode calculations
 */
class notPartOfIdentity extends StaticAnnotation

/**
 * This annotation explicitly excludes one constructor parameter from the copy method we generate, and can make a
 * concrete type extend concrete type mask values in the constructor
 * @see
 *   http://optimusdoc/InterAnnotations
 */
class copyAsIs extends StaticAnnotation

/**
 * @see
 *   http://optimusdoc/InterAnnotations
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
 * CM domain for upcasting, upcasting needs to be enabled for this domain in order for any entity to be upcasted to an
 * entity annotated with @upcastingTarget(CMUpcastDomain)
 */
case object CMUpcastDomain extends UpcastDomain {
  override val id = "cm"
}

/**
 * mark the annotated @stored @entity class as a target for upcasting if the given domain is enabled
 * @see
 *   http://optimusdoc/InterAnnotations
 */
sealed class upcastingTarget(val domain: UpcastDomain) extends StaticAnnotation

/**
 * During compilation, write diagnostics about graph nodes enclosed by this annotation to the given directory.
 */
final case class nodeDebug(dir: String)

/**
 * Trait Utils used by the meta annotation below
 */
// docs-snippet:OwnershipDefinition
final case class MetadataOwner(
    captain: String,
    additionalCaptains: Set[String],
    marshal: String,
    additionalMarshals: Set[String])
object MetadataOwner {
  def apply(captain: String, marshal: String): MetadataOwner =
    MetadataOwner(captain, Set.empty[String], marshal, Set.empty[String])
}
// docs-snippet:OwnershipDefinition
trait DalMetadata
trait OptOut extends DalMetadata
trait OwnershipMetadata {
  val owner: MetadataOwner
}

/**
 * Annotate a class with @meta to supply metadata for the CasC's manifest generator
 */
// docs-snippet:MetaDeclaration
class meta(
    /** Ownership - an object that extends OwnershipMetadata */
    val owner: OwnershipMetadata,
    /** Catalog - an object that extends DalMetadata */
    val catalog: DalMetadata
) extends StaticAnnotation {
// docs-snippet:MetaDeclaration
  def this(owner: OwnershipMetadata) = this(owner = owner, catalog = null)
  def this(catalog: DalMetadata) = this(owner = null, catalog = catalog)
}

// You don't want to use this - the intellij plugin injects it to help with highlighting, but it
// doesn't actually get compiled, and if it did it wouldn't do anything.
class ofInterestInIDE(val reason: String = "optimus") extends StaticAnnotation

/* Mark the @node a job, which brings enhanced node lifecycle observability through event publishing */
class job extends StaticAnnotation

/**
 * Nodes that are recursive and rely on tweaks to end the recursion cause cycles when XSFT is enabled. We have dynamic
 * cycle recovery for these cases but it's slow, and only runs on graph stall, so can affect batch sizes. Nodes that get
 * into this case are tracked with breadcrumbs (query: index=main source=RT payload.xsftCycle=*). For now, @recursive
 * will just revert to default caching for these nodes. In future we'll improve this so we re-enable XSFT when tweak
 * dependencies are learned once a node has run. Note this currently does nothing unless XSFT is enabled.
 */
class recursive extends StaticAnnotation
