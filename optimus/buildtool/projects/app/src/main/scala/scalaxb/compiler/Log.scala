/*
 * Copyright (c) 2010 e.e d3si9n
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

/* This is a patch of scalaxb's Log class, and replaces org.apache.log4j classes with non-deprecated loggers.
 * Notably, imports such as org.apache.log4j.Logger have been removed and replaced with a scala logger.
 * Log#fatal, Log#configureLogger and Log#Formatter have been removed because they were non-trivial to replace with alternatives
 *
 * For those changes only:
 *
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
package scalaxb.compiler

import msjava.slf4jutils.scalalog._

final case class Log(logger: Logger) {
  def info(message: String, args: Any*): Unit = {
    if (args.toSeq.isEmpty) logger.info(message)
    else
      try {
        logger.info(message format (args.toSeq: _*))
      } catch {
        case _: Throwable => logger.info(message)
      }
  }

  def debug(message: String, args: Any*): Unit = {
    if (args.toSeq.isEmpty) logger.debug(message)
    else
      try {
        logger.debug(message format (args.toSeq: _*))
      } catch {
        case _: Throwable => logger.debug(message)
      }
  }

  def warn(message: String, args: Any*): Unit = {
    if (args.toSeq.isEmpty) logger.warn(message)
    else
      try {
        logger.warn(message format (args.toSeq: _*))
      } catch {
        case _: Throwable => logger.warn(message)
      }
  }

  def error(message: String, args: Any*): Unit = {
    if (args.toSeq.isEmpty) logger.error(message)
    else
      try {
        logger.error(message format (args.toSeq: _*))
      } catch {
        case _: Throwable => logger.error(message)
      }
  }
}

object Log {
  def forName(name: String): Log = Log(getLogger(name))
}
