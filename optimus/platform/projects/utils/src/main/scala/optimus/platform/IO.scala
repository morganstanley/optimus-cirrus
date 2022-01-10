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

import org.slf4j.LoggerFactory

import scala.util.control.NonFatal
object IO {
  private lazy val log = LoggerFactory.getLogger(getClass)

  private def closeQuietly(c: AutoCloseable) =
    try if (c ne null) c.close()
    catch {
      case NonFatal(e) =>
        log.warn("Ignored exception while closing", e)
    }
  private def closeQuietly(closeFunction: () => Unit) =
    try if (closeFunction ne null) closeFunction()
    catch {
      case NonFatal(e) =>
        log.warn("Ignored exception while closing", e)
    }

  private def closeSilently(c: AutoCloseable) =
    try if (c ne null) c.close()
    catch { case NonFatal(_) => () }

  def using[A <: AutoCloseable, B](resource: A)(f: A => B): B = {
    var inFlight: Throwable = null
    try {
      f(resource)
    } catch {
      case t: Throwable =>
        inFlight = t
        throw t
    } finally {
      try resource.close()
      catch {
        case e: Exception =>
          if (inFlight eq null) throw e
          else inFlight.addSuppressed(e)
      }
    }
  }

  def using[A <: AutoCloseable, B <: AutoCloseable, C](resource1: A, resource2: B)(f: (A, B) => C): C = {
    using(resource1) { r1 =>
      using(resource2) { r2 =>
        f(r1, r2)
      }
    }
  }
  def using[A <: AutoCloseable, B <: AutoCloseable, C](resources: (A, B))(f: => C): C = {
    using(resources._1, resources._2)((_, _) => f)
  }

  def using[A <: AutoCloseable, B <: AutoCloseable, C <: AutoCloseable, D](resource1: A, resource2: B, resource3: C)(
      f: (A, B, C) => D): D = {
    using(resource1) { r1: A =>
      using(resource2) { r2: B =>
        using(resource3) { r3: C =>
          f(r1, r2, r3)
        }
      }
    }
  }
  def using[A <: AutoCloseable, B <: AutoCloseable, C <: AutoCloseable, D](resources: (A, B, C))(f: => D): D = {
    using(resources._1, resources._2, resources._3)((_, _, _) => f)
  }

  def using[A, B](resource: A, closeFunction: () => Unit)(f: A => B): B = {
    var inFlight: Throwable = null
    try {
      f(resource)
    } catch {
      case t: Exception =>
        inFlight = t
        throw t
    } finally {
      try closeFunction()
      catch {
        case e: Exception =>
          if (inFlight eq null) throw e
          else inFlight.addSuppressed(e)
      }
    }
  }

  def usingQuietly[A <: AutoCloseable, B](resource: A)(f: A => B): B = {
    try { f(resource) } finally { closeQuietly(resource) }
  }
  def usingQuietly[A, B](resource: A, closeFunction: () => Unit)(f: A => B): B = {
    try { f(resource) } finally { closeQuietly(closeFunction) }
  }

  def usingSilently[A <: AutoCloseable, B](resource: A)(f: A => B): B =
    try { f(resource) } finally { closeSilently(resource) }

}
