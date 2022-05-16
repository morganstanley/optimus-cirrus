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
package msjava.slf4jutils

/**
 * This package contains utility classes and methods to use the SLF4J logging API from scala. The three major components
 * contained within this package are:
 *   - Factory methods for obtaining a logger: [[msjava.slf4jutils.scalalog]].getLogger
 *   - An adapter for the SLF4J Logger interface: [[msjava.slf4jutils.scalalog.Logger]]
 *
 * Log statements are automatically guarded so that the arguments are not evaluated when the log layer is disabled.
 */
import scala.reflect.ClassTag
package object scalalog {
  import org.slf4j.{LoggerFactory => SLF4JLoggerFactory}
  import msjava.slf4jutils.scalalog.Logger
  import org.slf4j.ILoggerFactory

  private[this] var factory: ILoggerFactory = SLF4JLoggerFactory.getILoggerFactory

  // for testing
  private[scalalog] def setSlf4jLoggerFactory(loggerFactory: ILoggerFactory) = { factory = loggerFactory }

  /**
   * Factory method to obtain a [[msjava.slf4jutils.scalalog.Logger]] with any [[String]] as a name
   *
   * @param loggerName
   *   the string to use as the logger name
   */
  def getLogger(loggerName: String): Logger = {
    new Logger(factory.getLogger(loggerName))
  }
  def getLogger(loggerName: String, intercept: Intercept): Logger = {
    new Logger(factory.getLogger(loggerName), Some(intercept))
  }

  /**
   * Factory method to obtain a [[msjava.slf4jutils.scalalog.Logger]] from a Class[_] instance
   *
   * @param clazz
   *   Class[_] instance for which to name the [[msjava.slf4jutils.scalalog.Logger]]
   */
  def getLogger(clazz: Class[_]): Logger = {
    val className = clazz.getName
    val loggerName = if (className.last == '$') {
      className.substring(0, className.length - 1)
    } else {
      className
    }
    getLogger(loggerName)
  }

  /**
   * Factory method which should be used to obtain a [[msjava.slf4jutils.scalalog.Logger]] for Classes and Traits
   *
   * @tparam T
   *   type for which to name the [[msjava.slf4jutils.scalalog.Logger]]
   */
  def getLogger[T](implicit m: ClassTag[T]): Logger = {
    this.getLogger(m.runtimeClass)
  }

  /**
   * Factory method which should be used to obtain a [[msjava.slf4jutils.scalalog.Logger]] for Objects
   *
   * @param o
   *   object for which to name the [[msjava.slf4jutils.scalalog.Logger]]
   */
  def getLogger[T](o: T)(implicit m: ClassTag[T]): Logger = {
    this.getLogger(m.runtimeClass)
  }
}
