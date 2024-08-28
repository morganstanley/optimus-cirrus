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
package intellij.only

import optimus.platform._
import optimus.platform.node

//noinspection ScalaUnusedSymbol, NotImplementedCode
/* used by jetfire-pc so that the IJ presentation compiler knows how to convert between node functions and node annotated scala functions */
trait DoNotUseIJNodeFunctionConversions {

  implicit def Z_donotuse_nf0[R](f: () => R @node): NodeFunction0[R] = ???
  implicit def Z_donotuse_nf1[T, R](f: T => R @node): NodeFunction1[T, R] = ???
  implicit def Z_donotuse_nf2[T1, T2, R](f: (T1, T2) => R @node): NodeFunction2[T1, T2, R] = ???
  implicit def Z_donotuse_nf3[T1, T2, T3, R](f: (T1, T2, T3) => R @node): NodeFunction3[T1, T2, T3, R] = ???
  implicit def Z_donotuse_nf4[T1, T2, T3, T4, R](f: (T1, T2, T3, T4) => R @node): NodeFunction4[T1, T2, T3, T4, R] = ???
  implicit def Z_donotuse_nf5[T1, T2, T3, T4, T5, R](
      f: (T1, T2, T3, T4, T5) => R @node): NodeFunction5[T1, T2, T3, T4, T5, R] =
    ???
  implicit def Z_donotuse_nf6[T1, T2, T3, T4, T5, T6, R](
      f: (T1, T2, T3, T4, T5, T6) => R @node): NodeFunction6[T1, T2, T3, T4, T5, T6, R] = ???
  implicit def Z_donotuse_nf7[T1, T2, T3, T4, T5, T6, T7, R](
      f: (T1, T2, T3, T4, T5, T6, T7) => R @node): NodeFunction7[T1, T2, T3, T4, T5, T6, T7, R] = ???
  implicit def Z_donotuse_nf8[T1, T2, T3, T4, T5, T6, T7, T8, R](
      f: (T1, T2, T3, T4, T5, T6, T7, T8) => R @node): NodeFunction8[T1, T2, T3, T4, T5, T6, T7, T8, R] =
    ???
  implicit def Z_donotuse_nf9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R](
      f: (T1, T2, T3, T4, T5, T6, T7, T8, T9) => R @node): NodeFunction9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R] = ???

  implicit def Z_donotuse_af0[R](nf0: NodeFunction0[R]): () => R @node = ???
  implicit def Z_donotuse_af1[T, R](nf1: NodeFunction1[T, R]): T => R @node = ???
  implicit def Z_donotuse_af2[T1, T2, R](nf2: NodeFunction2[T1, T2, R]): (T1, T2) => R @node = ???
  implicit def Z_donotuse_af3[T1, T2, T3, R](nf3: NodeFunction3[T1, T2, T3, R]): (T1, T2, T3) => R @node = ???
  implicit def Z_donotuse_af4[T1, T2, T3, T4, R](nf4: NodeFunction4[T1, T2, T3, T4, R]): (T1, T2, T3, T4) => R @node =
    ???
  implicit def Z_donotuse_af5[T1, T2, T3, T4, T5, R](
      nf5: NodeFunction5[T1, T2, T3, T4, T5, R]): (T1, T2, T3, T4, T5) => R @node =
    ???
  implicit def Z_donotuse_af6[T1, T2, T3, T4, T5, T6, R](
      nf6: NodeFunction6[T1, T2, T3, T4, T5, T6, R]): (T1, T2, T3, T4, T5, T6) => R @node = ???
  implicit def Z_donotuse_af7[T1, T2, T3, T4, T5, T6, T7, R](
      nf7: NodeFunction7[T1, T2, T3, T4, T5, T6, T7, R]): (T1, T2, T3, T4, T5, T6, T7) => R @node = ???
  implicit def Z_donotuse_af8[T1, T2, T3, T4, T5, T6, T7, T8, R](
      nf8: NodeFunction8[T1, T2, T3, T4, T5, T6, T7, T8, R]): (T1, T2, T3, T4, T5, T6, T7, T8) => R @node =
    ???
  implicit def Z_donotuse_af9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R](
      nf9: NodeFunction9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R]): (T1, T2, T3, T4, T5, T6, T7, T8, T9) => R @node = ???
}
