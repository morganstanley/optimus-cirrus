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
package optimus.buildtool.runconf.compile

import optimus.buildtool.runconf.plugins.EnvInternal

import scala.collection.immutable.Seq

object Merger {
  def merge[A](target: Seq[A], source: Seq[A]): Seq[A] = {
    (target ++ source)
  }

  def merge(target: EnvInternal, source: EnvInternal): EnvInternal = {
    Maps.merge(target, source) {
      case (_, Left(source))              => Left(source)
      case (Right(target), Right(source)) => Right(target ++ source)
      case (Left(target), Right(source))  => Right(target +: source)
    }
  }

  def merge[B](target: Option[B], source: Option[B]): Option[B] = {
    source.orElse(target)
  }

  def mergeMaps[B](target: Map[B, B], source: Map[B, B]): Map[B, B] = {
    Maps.merge(target, source) { case (_, source) =>
      source
    }
  }
}

class Merger[A](target: A, source: A) {
  def merge[B](get: A => Seq[B]): Seq[B] = Merger.merge(get(target), get(source))
  def merge(get: A => EnvInternal): EnvInternal = Merger.merge(get(target), get(source))
  def merge[B](get: A => Option[B]): Option[B] = Merger.merge(get(target), get(source))
  def mergeDistinct[B](get: A => Seq[B]): Seq[B] = merge(get).distinct
  def mergeMaps[B](get: A => Map[B, B]): Map[B, B] = Merger.mergeMaps(get(target), get(source))
}
