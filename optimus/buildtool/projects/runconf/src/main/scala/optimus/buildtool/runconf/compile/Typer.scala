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

import scala.jdk.CollectionConverters._

private[compile] object Typer {

  def isApplicable(resolved: Type, expected: Type): Boolean = {
    resolved == expected ||
    expected == T.Any ||
    checkHigherKindedTypes(resolved, expected) ||
    resolved.superType.exists(isApplicable(_, expected))
  }

  private def checkHigherKindedTypes(resolved: Type, expected: Type): Boolean = {
    def checkEqualHKTypes(resolved: HigherKindedType, expected: HigherKindedType): Boolean = {
      resolved.inner == T.Nothing || isApplicable(resolved.inner, expected.inner)
    }
    (resolved, expected) match {
      case (Types.Union(resolvedTypes), Types.Union(expectedTypes)) =>
        resolvedTypes.forall(resolved => expectedTypes.exists(expected => isApplicable(resolved, expected)))
      case (other, Types.Union(expectedTypes)) =>
        expectedTypes.exists(isApplicable(other, _))
      case (Types.Union(resolvedTypes), other) =>
        resolvedTypes.forall(resolved => isApplicable(resolved, other))
      case (resolvedArr: Types.SizedArray, expectedArr: Types.SizedArray) =>
        resolvedArr.size == expectedArr.size && checkEqualHKTypes(resolvedArr, expectedArr)
      case (resolvedHK: HigherKindedType, expectedHK: HigherKindedType) if resolvedHK.getClass == expectedHK.getClass =>
        checkEqualHKTypes(resolvedHK, expectedHK)
      case _ => false
    }
  }

  def resolve(value: Any): Type = {
    value match {
      case null                 => T.Null
      case _: java.lang.Boolean => T.Boolean
      case _: java.lang.Integer => T.Integer
      case _: java.lang.Double  => T.Double
      case s: java.lang.String  => T.StringLiteral(s)
      case list: java.util.List[_] =>
        val types = list.asScala.map(resolve).distinct
        T.SizedArray(commonType(types), list.size)
      case map: java.util.Map[_, _] =>
        val types = map.values.asScala.map(resolve).toSeq.distinct
        T.Object(commonType(types))
      case any => throw new RuntimeException(s"Unexpected type: ${any.getClass}")
    }
  }

  private def commonType(elementTypes: Seq[Type]): Type = {
    elementTypes match {
      case Seq(single) => single
      case Seq()       => T.Nothing
      case multiple =>
        val objects = multiple.collect { case o: T.Object => o }
        val arrays = multiple.collect { case o: T.Array => o }
        if (objects == multiple) {
          val inners = commonType(objects.map(_.inner))
          T.Object(inners)
        } else if (arrays == multiple) {
          val inners = commonType(arrays.map(_.inner))
          T.Array(inners)
        } else {
          val allTypes = flattenUnions(multiple)
          T.Union(allTypes.toSet)
        }
    }
  }

  private def flattenUnions(multiple: Seq[Type]): Seq[Type] = {
    val unions = multiple.collect { case u: Types.Union => u }
    val nonUnions = multiple.filterNot(unions.contains)
    unions.flatMap(_.types) ++ nonUnions
  }

}
