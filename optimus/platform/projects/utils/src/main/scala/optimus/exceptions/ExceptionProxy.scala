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

import java.util.concurrent.atomic.AtomicInteger

import optimus.exceptions.config.ExceptionMatcher
import optimus.exceptions.config.RTListConfig
import optimus.utils.PropertyUtils
import org.slf4j.LoggerFactory

import scala.reflect.NameTransformer
import scala.util.matching.Regex

/**
 * The purpose of these objects is to catch otherwise over-general exceptions thrown by
 * legacy libraries that can't be immediately changed to throw more specific ones.
 * For example, if we had a root-solver function that threw
 *    RuntimeException("solution not within bounds")
 * we have reason to believe that is in fact a referentially-transparent result, one that
 * would occur every time the particular function arguments were passed, but we still don't
 * want to allow all Runtime Exceptions.
 * The example below would allow you to write
 *
 *    NodeTry {
 *      solve(f,a,b)
 *    } recover {
 *      case _:IllegalArgumentException => NaN
 *      case t@TestSolverExceptionProxy =>
 *         log.info(s"Harrumph: ${ t.getMessage} ")
 *         doSomethingCoverWithBounds()
 *    }
 *
 * The overridden equals method causes the case to match, while the derivation from
 * ExceptionProxy is treated by our macros as an exception from the usual rules.
 *
 * Note that these are loaded from [[RTList.ProxyConfig]] files; simply declaring one is no longer sufficient.
 */
trait ExceptionProxy extends Throwable with RTExceptionTrait {
  override def toString: String = getClass.getName stripSuffix NameTransformer.MODULE_INSTANCE_NAME
}

private[optimus] object ExceptionProxy {

  // This reflective nonsense is necessary, because utils does not depend on breadcrumbs.
  // We will eventually move the exception proxies out optimus.platform altogether.
  import scala.reflect.runtime.{universe => ru}
  private val send: ru.MethodMirror = {
    val mirror: ru.Mirror = ru.runtimeMirror(getClass.getClassLoader)
    val bcSym: ru.ModuleSymbol = mirror.staticModule("optimus.breadcrumbs.Breadcrumbs")
    val moduleMirror: ru.ModuleMirror = mirror.reflectModule(bcSym)
    val instanceMirror = mirror.reflect(moduleMirror.instance)
    val sendSymbol: ru.MethodSymbol =
      moduleMirror.symbol.typeSignature.decl(ru.TermName("emergencySendForReflection")).asMethod
    instanceMirror.reflectMethod(sendSymbol)
  }

  private val log = LoggerFactory.getLogger(this.getClass)

  val screamed = new AtomicInteger(0)

  def scream(proxy: ExceptionProxy, why: String, t: Throwable, n: Int): Unit = {
    screamed.incrementAndGet()
    val proxyName = proxy.getClass.getSimpleName
    val msg = t.getMessage.take(1000)
    val stack = t.getStackTrace.take(10).mkString(";")
    val newmsg = s"""$why; "$msg"; $proxyName[$n] """
    log.error(newmsg, t)
    send("ExceptionProxy", s"$newmsg $stack")
  }

}

private object Deprecated {
  val allowDeprecated: Boolean = PropertyUtils.get("optimus.exception.allow.deprecated", default = true)
  val maxScreams = 2
}

trait Deprecated {
  self: ExceptionProxy =>

  private var n = 0

  override def equals(obj: Any): Boolean = {
    val matched = super.equals(obj)
    matched && {
      n += 1
      if (n <= Deprecated.maxScreams) // shouldn't be necessary due to caching, but let's be careful about spam
        ExceptionProxy.scream(this, "Deprecated proxy matched!", obj.asInstanceOf[Throwable], n)
      Deprecated.allowDeprecated
    }
  }
}

object RuntimeExceptionWithMessage {
  def unapply(t: Throwable): Option[String] = t match {
    case t: RuntimeException => Option(t.getMessage)
    case _                   => None
  }
}

object ExceptionWithMessage {
  def unapply(t: Throwable): Option[String] = t match {
    case t: Exception => Option(t.getMessage)
    case _            => None
  }
}

class RuntimeExceptionMatching(matcher: Regex) extends Throwable {
  override def equals(obj: Any): Boolean = obj match {
    case RuntimeExceptionWithMessage(msg) => matcher.unapplySeq(msg).isDefined
    case _                                => false
  }
}

class RuntimeExceptionGreedyMatching(contains: String, matcher: Regex) extends Throwable {
  override def equals(obj: Any): Boolean = obj match {
    case RuntimeExceptionWithMessage(msg) => msg.contains(contains) && matcher.unapplySeq(msg).isDefined
    case _                                => false
  }
}

object TestSolverExceptionProxy
    extends RuntimeExceptionContaining("this is a bogus exception for testing")
    with ExceptionProxy

object TestDeprecatedExceptionProxy
    extends RuntimeExceptionContaining("this is a deprecated exception for testing")
    with ExceptionProxy
    with Deprecated

object TestRuntimeExceptionGreedyMatching
    extends RuntimeExceptionGreedyMatching("Start Date", """(?s).*(Start Date \( \d+ \) greater than end date.*)""".r)
    with ExceptionProxy

object TestRuntimeExceptionMatching
    extends RuntimeExceptionMatching("(?s).*(0 \\( 0 \\) > x \\( [-+]\\d+\\.\\d+ \\).*BLECCHO).*".r)
    with ExceptionProxy

class RuntimeExceptionContaining(contains: String) extends Throwable {
  override def equals(obj: Any): Boolean = obj match {
    case RuntimeExceptionWithMessage(msg) => msg.contains(contains)
    case _                                => false
  }
}

class ExceptionContaining(contains: String) extends Throwable {
  override def equals(obj: Any): Boolean = obj match {
    case ExceptionWithMessage(msg) => msg.contains(contains)
    case _                         => false
  }
}

object AdditionalExceptionProxy extends Throwable with ExceptionProxy {
  private val log = LoggerFactory.getLogger(this.getClass)

  val additions: Set[ExceptionMatcher] =
    RTListConfig.additionalRTExceptions
      .map(_.split(';').map(ExceptionMatcher.Parser.parse).toSet)
      .getOrElse(Set.empty[ExceptionMatcher])

  override def equals(obj: Any): Boolean = obj match {
    case t: Throwable =>
      val fqcn = t.getClass.getCanonicalName
      val msg = Option(t.getMessage).getOrElse(fqcn)
      val matchedMatcher = additions.find { matcher =>
        matcher.matchingCriterion.forall { _.matchWith(fqcn, msg) }
      }

      matchedMatcher.foreach { m =>
        log.warn(
          s"[RTList] added to allow-list dynamically: $fqcn (message=$msg) (via optimus.additional.rt.exceptions) (rule=${m})")
      }

      matchedMatcher.isDefined
    case _ => false
  }
}
