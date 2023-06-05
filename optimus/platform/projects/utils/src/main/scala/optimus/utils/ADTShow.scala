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

/**
 * Mixin to print algebraic data types with field names, i.e. Type(name1 = value1, name2 = value2, ...).
 *
 * {{{
 *   final case class Foo(i: Int, j: String)
 *   println(Foo(1, "meh")) // Foo(1,meh)
 *   final case class Bar(i: Int, j: String) extends ADTShow
 *   println(Bar(2, "hem")) // Bar(i = 2, j = hem)
 * }}}
 */
trait ADTShow extends Product with ProductFieldNames {
  override def toString: String = {
    val fields = productFieldNames zip productIterator map { case (n, v) => s"$n = $v" }
    if (fields.isEmpty)
      productPrefix
    else
      fields.mkString(s"$productPrefix(", ", ", ")")
  }

  def toStringMap: Map[String, String] = {
    productFieldNames zip productIterator map { case (k, v) => k -> v.toString } toMap
  }
}
