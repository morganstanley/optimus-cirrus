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
package optimus.buildtool.utils

import java.nio.file.Paths

import optimus.buildtool.files.Directory
import optimus.platform._
import optimus.utils.{TypeClasses => MoreTypeClasses}

import scala.collection.immutable.Seq

object TypeClasses extends MoreTypeClasses {
  implicit class MoreStringOps(s: String) extends StringOps(s) {
    def mapNonEmpty[A](f: String => A): Option[A] = if (s.nonEmpty) Some(f(s)) else None
    @async def mapNonEmpty$NF[A](f: NodeFunction[String, A]): Option[A] = if (s.nonEmpty) Some(f(s)) else None

    def getOrElse(v: => String): String = if (s.isEmpty) v else s
    @async def getOrElse(v: NodeFunction0[String]): String = if (s.isEmpty) v() else s

    def asDirectory: Option[Directory] = mapNonEmpty(s => Directory(Paths.get(s).toAbsolutePath))
  }

  implicit class SeqEitherOps[A, B](s: Seq[Either[A, B]]) {
    def separate: (Seq[A], Seq[B]) = s.foldLeft((Seq.empty[A], Seq.empty[B])) { case ((as, bs), e) =>
      e match {
        case Left(a)  => (as :+ a, bs)
        case Right(b) => (as, bs :+ b)
      }
    }
  }
}
