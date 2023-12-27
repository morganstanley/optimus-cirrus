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
package optimus.utils

import java.util.concurrent.atomic.AtomicReference

/**
 * WARNING Please use this class sparingly. It was added to port tests already using object mocking
 * to Scala 2.13, it is not an endorsement of writing more tests that rely on this pattern!
 *
 * Subclass this as per the example below to support test cases that need to inject a mock instance.
 *
 * In Scala 2.12, mockito was able to transparently mock any Scala object by using reflection to mutate
 * the field the contains the singleton instance. However in Scala 2.13, the encoding of objects slightly
 * changed to conform to JVM specification.
 *
 * Before:
 * {{{
 * class Foo {
 *   def foo = 42
 *   object Inner
 * }
 * }}}
 *
 * After:
 * {{{
 * object Foo extends MockableObject[FooCompanion]
 * class FooCompanion {
 *   def foo = 42
 *   object Inner
 * }
 * }}}
 *
 * Test code:
 *
 * {{{
 *   val mockFooCompanion = mock[FooCompanion]
 *   Foo.withMock(mockFooCompanion) {
 *      mockFooCompanion.foo returns -1
 *      methodUnderTest()
 *   }
 * }}}
 *
 * An implicit conversion is provided from `Foo` to `FooCompanion` so most client code works without change.
 * One exception is
 * {{{
 *   import Foo.Inner // implicit conversion can't make this work
 * }}}
 *
 * The recommended refactoring is to move the method that need to me mocked into a dedicated object and delegate
 * to it from the original methods.
 *
 * Related:
 * https://bugs.openjdk.org/browse/JDK-8159215
 * https://github.com/scala/scala/pull/7270
 *
 * @tparam T
 */
trait MockableObject[T <: AnyRef] {
  private val instance = new AtomicReference[T](create)
  def apply(): T = {
    instance.get
  }
  def withMock[A](t: T)(f: => A): A = {
    val saved = instance.getAndSet(t)
    try f
    finally instance.set(saved)
  }
  protected def create: T
  implicit def toT(self: this.type): T = apply()
}
