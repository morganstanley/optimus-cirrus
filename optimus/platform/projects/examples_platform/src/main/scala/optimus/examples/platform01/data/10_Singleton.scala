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

import optimus.examples.platform.entities.SimpleEvent
import optimus.platform._

/**
 * Demonstrates singleton entity providing on-graph helper functions - TODO (OPTIMUS-0000): Update as syntax improves in future
 * releases.
 *
 *   - WARNING: The syntax will change in future releases:
 *     - We don't support ''singleton'' entity keys or any `@stored @entity object` or singleton access API syntax yet.
 *     - See [[OPTIMUS-80]]
 *   - Added example of how to achieve this now because Reha needs to do it in sample risk app.
 *
 * Reha asked for:
 *   - Scala object like entity that provides helper functions
 *   - Entity can be stored, so any config for the helper functions is taken from data-store (bi-temporal)
 */
/**
 * When we support properly, would be some way of indicating singleton key.
 *   - eg `@singleton`, or `@stored @entity object X{...}` with ability to DAL.put mutated
 */
@stored @entity class ExampleSingleton(@node val someConfig: Double) {

  /**
   * Prefix with '_' to avoid any clash in companion object between property meta-data and singleton forwarder. Any real
   * support for singletons would not need user to do things like this.
   */
  @node(tweak = true) def _utilFunc(param: Double) = param + someConfig

  @key val name: Unit = ()
}

/**
 * When we support properly, Entity Scalac plugin would generate any boilerplate.
 */
object ExampleSingleton {
  def apply(): ExampleSingleton = ExampleSingleton(5.0)

  /**
   * Forwarding method (scalac plugin would generate ;-)
   */
  def utilFunc(param: Double) = instance._utilFunc(param)

  /// Would expect a real impl to do better caching.
  /// WARNING: Current DAL doesn't have ANY caching for DAL.get
  def instance: ExampleSingleton = ExampleSingleton.get(())

  /// Init for standalone example
  def ensureStored(): Unit = {
    ExampleSingleton.getOption(()) getOrElse {
      newEvent(SimpleEvent.uniqueInstance("CreateSingleton")) {
        DAL.put(ExampleSingleton())
      }
    }
  }
}

object Singleton extends OptimusApp {
  val user = System.getProperty("user.name")

  override def setup(): Unit = {
    if (cmdLine.uri == null)
      cmdLine.uri = "broker://dev?context=named&context.name=" + user
  }

  @entersGraph override def run(): Unit = {
    println(
      "NOTE: Syntax and support for singleton like entities that provide on-graph helper functions will be improved in future releases.")
    println()
    ExampleSingleton.ensureStored()

    given(validTimeAndTransactionTimeNow) {
      println(ExampleSingleton.utilFunc(1.0), ExampleSingleton.utilFunc(2.3))

      // Show how to tweak a helper function on a singleton entity
      // WARNINIG: In current release DAL.get has NO caching, so tweak won't match on an instance (so match on any instance)
      // NOTE: Only using predicate here because tweak-target to match a property call with args on any entity instance is not yet supported.
      given(ExampleSingleton._utilFunc when { (e, p) =>
        p == 1.0
      } := -99) {
        println(ExampleSingleton.utilFunc(1.0), ExampleSingleton.utilFunc(2.3))
      }
    }
  }
}
