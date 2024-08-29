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
package optimus.platform.pickling

import java.time.LocalDate
import optimus.utils.datetime.OrderingImplicits._
import optimus.scalacompat.collection._

import scala.reflect.runtime.universe._

class DefaultOrderingFactory extends OrderingFactory {

  override protected lazy val values: Map[ClassKey, List[Type] => Option[Ordering[_]]] = Map(
    /* no type parameter */
    classKeyOf[Unit] -> typeParams0(Ordering.Unit),
    classKeyOf[Boolean] -> typeParams0(Ordering.Boolean),
    classKeyOf[Byte] -> typeParams0(Ordering.Byte),
    classKeyOf[Char] -> typeParams0(Ordering.Char),
    classKeyOf[Short] -> typeParams0(Ordering.Short),
    classKeyOf[Int] -> typeParams0(Ordering.Int),
    classKeyOf[Long] -> typeParams0(Ordering.Long),
    classKeyOf[Float] -> typeParams0(FloatOrdering),
    classKeyOf[Double] -> typeParams0(DoubleOrdering),
    classKeyOf[BigInt] -> typeParams0(Ordering.BigInt),
    classKeyOf[BigDecimal] -> typeParams0(Ordering.BigDecimal),
    classKeyOf[String] -> typeParams0(Ordering.String),
    classKeyOf[java.time.Instant] -> typeParams0(Ordering[java.time.Instant]),
    classKeyOf[java.time.LocalDate] -> typeParams0(Ordering[java.time.LocalDate]),
    /* one type parameter */
    classKeyOf[Option[_]] -> typeParams1(t => Ordering.Option(t)),
    classKeyOf[Iterable[_]] -> typeParams1(t => Ordering.Iterable(t)),
    /* two or more type parameters */
    classKeyOf[(_, _)] -> typeParams2((a, b) => Ordering.Tuple2(a, b)),
    classKeyOf[(_, _, _)] -> typeParams3((a, b, c) => Ordering.Tuple3(a, b, c)),
    classKeyOf[(_, _, _, _)] -> typeParams4((a, b, c, d) => Ordering.Tuple4(a, b, c, d)),
    classKeyOf[(_, _, _, _, _)] -> typeParams5((a, b, c, d, e) => Ordering.Tuple5(a, b, c, d, e)),
    classKeyOf[(_, _, _, _, _, _)] -> typeParams6((a, b, c, d, e, f) => Ordering.Tuple6(a, b, c, d, e, f)),
    classKeyOf[(_, _, _, _, _, _, _)] -> typeParams7((a, b, c, d, e, f, g) => Ordering.Tuple7(a, b, c, d, e, f, g)),
    classKeyOf[(_, _, _, _, _, _, _, _)] -> typeParams8((a, b, c, d, e, f, g, h) =>
      Ordering.Tuple8(a, b, c, d, e, f, g, h)),
    classKeyOf[(_, _, _, _, _, _, _, _, _)] -> typeParams9((a, b, c, d, e, f, g, h, i) =>
      Ordering.Tuple9(a, b, c, d, e, f, g, h, i))
  )

  override protected lazy val superclassValues: Map[ClassKey, Type => Option[Ordering[_]]] = Map(
    classKeyOf[Enumeration#Value] -> (_ => Some(new Enumeration {}.ValueOrdering.asInstanceOf[Ordering[_]]))
  )
}
