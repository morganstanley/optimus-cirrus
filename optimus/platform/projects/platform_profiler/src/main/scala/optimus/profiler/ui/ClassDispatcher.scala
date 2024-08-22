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
package optimus.profiler.ui
import scala.reflect.ClassTag

/**
 * Basically a typesafe `Map[Class[A], F[A]]` where the key and value types A always match for a given pair.
 *
 * [[get]] and [[apply]] returns the F[_] defined for the class of the value that is passed in.
 */
class ClassDispatcher[F[_]](private val underlying: Map[Class[_], F[_]]) {
  def get[T](value: T): Option[F[T]] = underlying.get(value.getClass).asInstanceOf[Option[F[T]]]
  def apply[T](value: T): F[T] = underlying(value.getClass).asInstanceOf[F[T]]
  def contains(cls: Class[_]): Boolean = underlying.contains(cls)
}

object ClassDispatcher {
  final case class Builder[F[-_]](
      private val builder: Map[Class[_], F[_]] = Map.empty[Class[_], F[_]]
  ) {

    def put[T: ClassTag](f: F[T]): Builder[F] = copy(
      builder = builder + (implicitly[ClassTag[T]].runtimeClass -> f)
    )

    def result(): ClassDispatcher[F] = new ClassDispatcher(builder)
  }

  def newBuilder[F[-_]] = new Builder[F]
}
