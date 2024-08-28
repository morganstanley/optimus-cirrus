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
package optimus.graph

/**
 * This class serves as resource handler for C++-side objects in a monadic style. The goal is to manipulate C++-side
 * objects in a so that the associated memory is freed after usage.
 *
 * e.g. for { blah <- Disposer(makeBlah())(_.delete()) blee <- methodThatAlreadyReturnsDisposer() } yield foo(blah,
 * blee)
 */

trait Disposable {
  def dispose(): Unit
}

trait Disposer[V] { self =>
  import Disposer._

  protected def toCpp: V
  protected def deleteCpp(arg: V): Unit

  def foreach(consumer: V => Unit): Unit = map(consumer).get
  def flatMap[R](mapper: V => Isolate[R]): Isolate[R] = map(mapper(_).get)
  def map[R](mapper: V => R): Isolate[R] = new Isolate[R](() => {
    val cppObj = self.toCpp
    try {
      mapper(cppObj)
    } finally {
      self.deleteCpp(cppObj)
    }
  })
}

object Disposer {
  // Exists to force use of Disposer#flatMap outside of for comprehensions where the inner comes from #map
  final class Isolate[V] private[Disposer] (construct: () => V) { self =>
    private[Disposer] def get: V = construct()
    def disposer(deleter: V => Unit): Disposer[V] = Disposer[V](self.get, deleter)
    private[Disposer] def foreach(consumer: V => Unit): Unit = map(consumer).get
    private[Disposer] def map[R](f: V => R): Isolate[R] = new Isolate[R](() => f(self.get))
    private[Disposer] def flatMap[R](f: V => Isolate[R]): Isolate[R] = f { self.get }
  }

  def apply[V](construct: => V, delete: V => Unit): Disposer[V] = new Disposer[V] {
    override protected def toCpp: V = construct
    override protected def deleteCpp(arg: V): Unit = delete(arg)
  }

  def apply[V <: Disposable](construct: => V): Disposer[V] = new Disposer[V] {
    override protected def toCpp: V = construct
    override protected def deleteCpp(arg: V): Unit = arg.dispose()
  }

}
