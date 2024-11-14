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
package optimus.examples.platform01.data

import optimus.platform._
import optimus.examples.platform.entities.SimpleCalcs

@entity
trait XEntityTrait {
  @node def x: Int
}

@entity
trait AEntityTrait {
  @node def a: Int
  @node def f(i: Int): Int
}

@entity
trait XAEntityTrait extends XEntityTrait with AEntityTrait

@entity
trait YEntityTrait {
  @node def y: Int
  // TODO (OPTIMUS-0000): Support: @node val y: Int
}

/**
 * An @stored @entity class that extends some `@stored @entity trait`s (interface style).
 *
 * Shows that @node vals and defs can implement abstract defs from traits.
 */
@entity
class AnEntityExtendingEntityTraits(@node val x: Int = 7, @node val y: Int = 0)
    extends XAEntityTrait
    with YEntityTrait {
  @node def a = x * 2 + y
  @node def f(i: Int) = 2 * i
}

trait XTrait {
  @node def x: Int
}

trait ATrait {
  @node def a: Int
  @node def f(i: Int): Int
}

trait XATrait extends XTrait with ATrait

trait YTrait {
  @node def y: Int
}

/**
 * An @stored @entity class that extends some vanilla Scala interface style traits.
 *
 * Shows that @node vals and defs can implement abstract defs from traits.
 */
@entity
class AnEntityExtendingScalaTraits(@node val x: Int = 7, @node val y: Int = 0) extends XATrait with YTrait {
  @node def a = x * 2 + y
  @node def f(i: Int) = 2 * i
}

object EntitiesAndTraits extends LegacyOptimusApp {

  def showInterfaces(c: Class[_], indent: String = ""): Unit = {
    println(indent + c.getName)
    for (i <- c.getInterfaces if i.getName.contains("examples")) {
      showInterfaces(i, indent + "  ")
    }
  }

  val e = AnEntityExtendingEntityTraits()
  println((e, e.x, e.y, e.a))

  showInterfaces(e.getClass)

  val t = AnEntityExtendingScalaTraits()
  println((t, t.x, t.y, t.a))

  showInterfaces(t.getClass)
}
