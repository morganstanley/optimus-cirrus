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
package optimus.platform.dsi.bitemporal

import java.util.UUID

import scala.util.Failure
import scala.util.Success
import scala.util.Try

sealed trait ContextType {
  def id: String
}

case object DefaultContextType extends ContextType {
  override val id: String = "default"
}

case object NamedContextType extends ContextType {
  override val id: String = "named"
}

case object SharedContextType extends ContextType {
  override val id: String = "shared"
}

case object UniqueContextType extends ContextType {
  override val id: String = "unique"
}

sealed trait Context {
  def contextType: ContextType
  def contextName: Option[String] = None
  // This is used as a representation in ZK nodes etc. Please don't change it without understanding the ramifications.
  def id: String =
    contextName
      .map { n =>
        s"${contextType.id}.${n}"
      }
      .getOrElse(contextType.id)
}

case object DefaultContext extends Context {
  def fromJava() = DefaultContext
  override val contextType: ContextType = DefaultContextType
}

final case class NamedContext(name: String) extends Context {
  override def contextType: ContextType = NamedContextType
  override def contextName = Some(name)
  def sanitizedName(): String = name.replaceAll("[\\W-]", "Q")
  def validate(): Boolean = !name.matches(".*[^\\w-].*")
}

final case class SharedContext(name: String) extends Context {
  override def contextType: ContextType = SharedContextType
  override def contextName = Some(name)
}

final case class UniqueContext(uuid: UUID = UUID.randomUUID) extends Context {
  override def contextType: ContextType = UniqueContextType
  val contextCreationTime = patch.MilliInstant.now.toEpochMilli
}

object Context {
  private val ContextPattern = "^(\\w+)(?:(?:\\.)([\\w-]+))?$".r

  private[optimus] def apply(context: Option[String], contextName: Option[String] = None): Context =
    (context, contextName) match {
      case (None, None)    => DefaultContext
      case (None, Some(n)) => NamedContext(n)
      case (Some(id), _)   => parse(parseType(id.toLowerCase), contextName).get
    }

  private[optimus] def apply(contextStr: String): Context = {
    val (context, contextName) = parseContextString(contextStr)
    apply(Some(context), contextName)
  }

  private[optimus] def apply(args: Map[String, String]): Context = {
    apply(args.get("context"), args.get("context.name") map validateContextName)
  }

  private def parseContextString(context: String): (String, Option[String]) = {
    // must match: "default", "named.contextName" "unique.uuid"
    context match {
      case ContextPattern(id, name) => (id, Option(name))
      case _ => throw new IllegalArgumentException(s"Invalid context string, should contain one or two parts: $context")
    }
  }

  private def validateContextName(name: String): String = {
    if (!NamedContext(name).validate())
      throw new IllegalArgumentException(s"Context name '$name' contains invalid characters")
    else name
  }

  /**
   * Parses a string to it's corresponding context type
   * @param contextTypeId
   *   \- context type id that must match the id of an existing ContextType object
   * @return
   */
  private[optimus] def parseType(contextTypeId: String): ContextType = contextTypeId match {
    case DefaultContextType.id => DefaultContextType
    case NamedContextType.id   => NamedContextType
    case SharedContextType.id  => SharedContextType
    case UniqueContextType.id  => UniqueContextType
    case other                 => throw new IllegalArgumentException(s"Unknown context type '$other'")
  }

  private def parse(contextType: ContextType, name: Option[String], enforceSomeUuid: Boolean = false): Try[Context] =
    (contextType, name) match {
      case (DefaultContextType, None)                    => Success(DefaultContext)
      case (UniqueContextType, Some(u))                  => Success(UniqueContext(UUID.fromString(u)))
      case (UniqueContextType, None) if !enforceSomeUuid => Success(UniqueContext())
      case x @ (UniqueContextType, None)                 => throw new MatchError(x)
      case (NamedContextType, Some(n))                   => Success(NamedContext(n))
      case (SharedContextType, Some(n))                  => Success(SharedContext(n))
      case (DefaultContextType, Some(_)) | (NamedContextType, None) | (SharedContextType, None) =>
        Failure(new IllegalArgumentException(s"Invalid Invalid context: id=${contextType.id}, name=$name"))
    }

  /**
   * Parses a context string representation, that should match the following format contextTypeId?.nameOrUuid
   *
   * @param contextStr
   *   \- context str
   * @return
   *   contextInstance
   */
  private[optimus] def parseContext(contextStr: String): Try[Context] = {
    Try(parseContextString(contextStr))
      .flatMap { case (id, name) => parse(parseType(id), name, enforceSomeUuid = true) }
  }
}
