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
package optimus.platform.relational.asm;

import optimus.graph.Node;
import optimus.graph.NodeClsID;
import scala.Function0;
import scala.Function1;
import scala.Function2;
import scala.Function3;
import scala.Function4;
import scala.Function5;
import scala.Function6;
import scala.Function7;
import scala.Function8;
import scala.Function9;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;
import scala.runtime.AbstractFunction4;
import scala.runtime.AbstractFunction5;
import scala.runtime.AbstractFunction6;
import scala.runtime.AbstractFunction7;
import scala.runtime.AbstractFunction8;
import scala.runtime.AbstractFunction9;

public abstract class FuncUtils {
  public static final class LF0<R> extends AbstractFunction0<Node<R>>
      implements NodeClsID, LiftedFunction {
    private static int __nclsID = 0;
    private final Function0<Node<R>> f;

    public LF0(Function0<Node<R>> f) {
      this.f = f;
    }

    public int getClsID() {
      return __nclsID;
    }

    public Node<R> apply() {
      return f.apply();
    }
  }

  public static final class LF1<T, R> extends AbstractFunction1<T, Node<R>>
      implements NodeClsID, LiftedFunction {
    private static int __nclsID = 0;
    private final Function1<T, Node<R>> f;

    public LF1(Function1<T, Node<R>> f) {
      this.f = f;
    }

    public int getClsID() {
      return __nclsID;
    }

    public Node<R> apply(T t) {
      return f.apply(t);
    }
  }

  public static final class LF2<T1, T2, R> extends AbstractFunction2<T1, T2, Node<R>>
      implements NodeClsID, LiftedFunction {
    private static int __nclsID = 0;
    private final Function2<T1, T2, Node<R>> f;

    public LF2(Function2<T1, T2, Node<R>> f) {
      this.f = f;
    }

    public int getClsID() {
      return __nclsID;
    }

    public Node<R> apply(T1 t1, T2 t2) {
      return f.apply(t1, t2);
    }
  }

  public static final class LF3<T1, T2, T3, R> extends AbstractFunction3<T1, T2, T3, Node<R>>
      implements NodeClsID, LiftedFunction {
    private static int __nclsID = 0;
    private final Function3<T1, T2, T3, Node<R>> f;

    public LF3(Function3<T1, T2, T3, Node<R>> f) {
      this.f = f;
    }

    public int getClsID() {
      return __nclsID;
    }

    public Node<R> apply(T1 t1, T2 t2, T3 t3) {
      return f.apply(t1, t2, t3);
    }
  }

  public static final class LF4<T1, T2, T3, T4, R>
      extends AbstractFunction4<T1, T2, T3, T4, Node<R>> implements NodeClsID, LiftedFunction {
    private static int __nclsID = 0;
    private final Function4<T1, T2, T3, T4, Node<R>> f;

    public LF4(Function4<T1, T2, T3, T4, Node<R>> f) {
      this.f = f;
    }

    public int getClsID() {
      return __nclsID;
    }

    public Node<R> apply(T1 t1, T2 t2, T3 t3, T4 t4) {
      return f.apply(t1, t2, t3, t4);
    }
  }

  public static final class LF5<T1, T2, T3, T4, T5, R>
      extends AbstractFunction5<T1, T2, T3, T4, T5, Node<R>> implements NodeClsID, LiftedFunction {
    private static int __nclsID = 0;
    private final Function5<T1, T2, T3, T4, T5, Node<R>> f;

    public LF5(Function5<T1, T2, T3, T4, T5, Node<R>> f) {
      this.f = f;
    }

    public int getClsID() {
      return __nclsID;
    }

    public Node<R> apply(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) {
      return f.apply(t1, t2, t3, t4, t5);
    }
  }

  public static final class LF6<T1, T2, T3, T4, T5, T6, R>
      extends AbstractFunction6<T1, T2, T3, T4, T5, T6, Node<R>>
      implements NodeClsID, LiftedFunction {
    private static int __nclsID = 0;
    private final Function6<T1, T2, T3, T4, T5, T6, Node<R>> f;

    public LF6(Function6<T1, T2, T3, T4, T5, T6, Node<R>> f) {
      this.f = f;
    }

    public int getClsID() {
      return __nclsID;
    }

    public Node<R> apply(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6) {
      return f.apply(t1, t2, t3, t4, t5, t6);
    }
  }

  public static final class LF7<T1, T2, T3, T4, T5, T6, T7, R>
      extends AbstractFunction7<T1, T2, T3, T4, T5, T6, T7, Node<R>>
      implements NodeClsID, LiftedFunction {
    private static int __nclsID = 0;
    private final Function7<T1, T2, T3, T4, T5, T6, T7, Node<R>> f;

    public LF7(Function7<T1, T2, T3, T4, T5, T6, T7, Node<R>> f) {
      this.f = f;
    }

    public int getClsID() {
      return __nclsID;
    }

    public Node<R> apply(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6, T7 t7) {
      return f.apply(t1, t2, t3, t4, t5, t6, t7);
    }
  }

  public static final class LF8<T1, T2, T3, T4, T5, T6, T7, T8, R>
      extends AbstractFunction8<T1, T2, T3, T4, T5, T6, T7, T8, Node<R>>
      implements NodeClsID, LiftedFunction {
    private static int __nclsID = 0;
    private final Function8<T1, T2, T3, T4, T5, T6, T7, T8, Node<R>> f;

    public LF8(Function8<T1, T2, T3, T4, T5, T6, T7, T8, Node<R>> f) {
      this.f = f;
    }

    public int getClsID() {
      return __nclsID;
    }

    public Node<R> apply(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6, T7 t7, T8 t8) {
      return f.apply(t1, t2, t3, t4, t5, t6, t7, t8);
    }
  }

  public static final class LF9<T1, T2, T3, T4, T5, T6, T7, T8, T9, R>
      extends AbstractFunction9<T1, T2, T3, T4, T5, T6, T7, T8, T9, Node<R>>
      implements NodeClsID, LiftedFunction {
    private static int __nclsID = 0;
    private final Function9<T1, T2, T3, T4, T5, T6, T7, T8, T9, Node<R>> f;

    public LF9(Function9<T1, T2, T3, T4, T5, T6, T7, T8, T9, Node<R>> f) {
      this.f = f;
    }

    public int getClsID() {
      return __nclsID;
    }

    public Node<R> apply(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6, T7 t7, T8 t8, T9 t9) {
      return f.apply(t1, t2, t3, t4, t5, t6, t7, t8, t9);
    }
  }
}
