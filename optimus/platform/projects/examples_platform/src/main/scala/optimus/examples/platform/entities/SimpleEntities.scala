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
package optimus.examples.platform.entities

import optimus.platform._

// our simple entity will have a few stored properties
@stored @entity
class SimpleEntity(@key val name: String) {
  @entersGraph def amount$init(): Int = 0
  @entersGraph def value$init(): Double = 0.0

  // stored properties - store data
  @node(tweak = true) val amount: Int = 0
  @node(tweak = true) val value: Double = 0.0
  @node(tweak = true) def f(i: Int, j: Int) = i * j
}

@stored @entity
class SimpleIndexedEntity(@key val name: String, @indexed val city: String)

@stored @entity
class SimpleChildEntity(@key val name: String)

@stored @entity
class SimpleParentEntity(@key val name: String, @node val child: SimpleChildEntity)

@event class SimpleEvent(@key val name: String)
