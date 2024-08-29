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
package optimus.platform.bitemporal

object DateConversions {
  // Taken from http://scala-programming-language.1934581.n4.nabble.com/scala-implicit-def-comparable2ordered-td2001912.html
  // TODO (OPTIMUS-57177): license might be needed in order to publish this code as open-source
  implicit def comparableToOrdered[A <: Comparable[A]](x: A): Ordered[A] =
    new Ordered[A] with Proxy {
      val self = x
      def compare(y: A): Int = { x.compareTo(y) }
    }
}
