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

private[compile] sealed trait Type {
  def name: String
  def superType: Option[Type]
}

private[compile] class SimpleType(
    val name: String,
    val superType: Option[Type] = None
) extends Type {
  override def toString: String = name
}

private[compile] class HigherKindedType(
    val name: String,
    val inner: Type,
    val superType: Option[Type] = None
) extends Type {
  override def toString: String = s"$name(${inner.toString})"
}

private[compile] object Types {
  object Any extends SimpleType("any")
  object Nothing extends SimpleType("nothing")
  object Null extends SimpleType("null")
  object Boolean extends SimpleType("boolean")
  object Integer extends SimpleType("integer")
  object Double extends SimpleType("double")
  object String extends SimpleType("string")
  final case class Union(types: Set[Type]) extends SimpleType(types.toList.sortBy(_.toString).mkString(" | "))
  final case class StringLiteral(value: String) extends SimpleType(s"string($value)", superType = Some(String))
  final case class Array(override val inner: Type) extends HigherKindedType("array", inner)
  final case class SizedArray(override val inner: Type, size: Int)
      extends HigherKindedType(s"array[$size]", inner, superType = Some(Array(inner)))
  final case class Object(override val inner: Type) extends HigherKindedType("object", inner)
}
