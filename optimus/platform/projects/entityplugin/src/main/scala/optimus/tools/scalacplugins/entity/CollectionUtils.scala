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
package optimus.tools.scalacplugins.entity

import optimus.scalacompat.collection._
import optimus.scalacompat.collection.BuildFrom

private[scalacplugins] object CollectionUtils {

  /*
  These classes deliberately replicate functionality in utils, to avoid depending on it,
  which in makes it less likely that entityplugin will be rebuilt unnecessarily.
   */

  implicit final class EndoListSync[A](val as: List[A]) extends AnyVal {

    def :?:[B >: A](bo: Option[B]): List[B] = bo match {
      case Some(b) =>
        b :: as
      case None =>
        as
    }

    def :?:[B >: A <: AnyRef](b: B): List[B] = if (b ne null) b :: as else as
  }

  implicit final class EndoFunctionSync[T](val t: T) extends AnyVal {
    def applyIf[U >: T](cond: Boolean)(f: T => U): U =
      if (cond) f(t) else t
  }

  implicit class SingleCollection[A](coll: Iterable[A]) {
    def single: A = {
      val count = coll.size
      if (count == 1) {
        coll.head
      } else {
        throw new IllegalArgumentException(s"collection must contain exactly 1 element, found $count elements")
      }
    }
  }

}
