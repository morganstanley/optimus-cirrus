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
package optimus.platform;

import java.util.Objects;

import optimus.graph.AlreadyCompletedNode;
import optimus.graph.AlreadyCompletedPropertyNode;
import optimus.graph.CorePluginSupport$;
import optimus.graph.InitedPropertyNodeSync;
import optimus.graph.Node;
import optimus.graph.NodeTrace;
import optimus.graph.OptimusCancellationException;
import optimus.graph.PropertyNode;
import optimus.graph.loom.*;
import optimus.platform.storable.Entity;
import optimus.platform.storable.Storable;
import optimus.platform.temporalSurface.TemporalSurface;
import scala.Function0;
import scala.Function1;
import scala.Function2;

@SuppressWarnings("unchecked")
public class PluginHelpers {

  public static int hashOf(int prev, boolean v) {
    return prev * 31 + (v ? 1231 : 1237);
  }

  public static int hashOf(int prev, byte v) {
    return prev * 31 + v;
  }

  public static int hashOf(int prev, short v) {
    return prev * 31 + v;
  }

  public static int hashOf(int prev, char v) {
    return prev * 31 + v;
  }

  public static int hashOf(int prev, int v) {
    return prev * 31 + v;
  }

  public static int hashOf(int prev, long v) {
    return prev * 31 + (int) (v ^ (v >>> 32));
  }

  public static int hashOf(int prev, double v) {
    long bits = Double.doubleToLongBits(v);
    return prev * 31 + (int) (bits ^ (bits >>> 32));
  }

  public static int hashOf(int prev, float v) {
    return prev * 31 + Float.floatToIntBits(v);
  }

  public static int hashOf(int prev, Object v) {
    return prev * 31 + (v == null ? 0 : v.hashCode());
  }

  /**
   * used in hashcode generation of an @embeddable/@stable. 0 is the initial field value, and non
   * zero is required as a result, to indicate is a calculated value
   *
   * @param hash the calculated hash
   * @return the hash value to be stored
   */
  public static int avoidZero(int hash) {
    return (hash == 0) ? -1 : hash;
  }

  /**
   * Used in @embeddable/@stable equals. Compare the hashcode. If the hashcode is available then
   * compare, if not then ignore Either of the hashes may be 0 (not calculated yet), so in that case
   * hash cant be used usefully
   *
   * @param hash1 the stored hash of the first case class
   * @param hash2 the stored hash of the first case class
   * @return false iff the hashes are known to be not equal, true otherwise
   */
  public static boolean equalsAvoidingZero(int hash1, int hash2) {
    return hash1 == hash2 || hash1 == 0 || hash2 == 0;
  }

  /* use DoubleCompareBenchmark when considering other implementations
   * Note - equals(Float.NaN, Float.NaN) == true */
  public static boolean equals(float f1, float f2) {
    return f1 == f2 || (f1 != f1 && f2 != f2);
  }

  /* use DoubleCompareBenchmark when considering other implementations
   * Note - equals(Double.NaN, Double.NaN) == true */
  public static boolean equals(double d1, double d2) {
    return d1 == d2 || (d1 != d1 && d2 != d2);
  }

  public static boolean equals(byte p1, byte p2) {
    return p1 == p2;
  }

  public static boolean equals(short p1, short p2) {
    return p1 == p2;
  }

  public static boolean equals(int p1, int p2) {
    return p1 == p2;
  }

  public static boolean equals(long p1, long p2) {
    return p1 == p2;
  }

  public static boolean equals(boolean p1, boolean p2) {
    return p1 == p2;
  }

  public static boolean equals(Object p1, Object p2) {
    return Objects.equals(p1, p2);
  }

  /**
   * some quick checks on equality this is used when we have many reference types to check, e.g. in
   * the args of a case class for equality where we call canEqual on each first, and then equals
   * Calls to this family of method ore overloaded to allow the compiler to chose the most
   * appropriate method
   */
  public static boolean canEqualStorable(Storable p1, Storable p2) {
    if (p1 == p2) return true;
    if (p1 == null || p2 == null) return false;
    // fix this to be Storable - cache the hashcode
    if (p1 instanceof Entity)
      return p1.getClass() == p2.getClass() && p1.hashCode() == p2.hashCode();
    return p1.getClass() == p2.getClass();
  }

  public static Object safeResult(Node<?> node) {
    if (node instanceof AlreadyCompletedPropertyNode) {
      return node.result();
    } else if (node instanceof InitedPropertyNodeSync<?> syncNode) {
      TemporalContext tc = syncNode.entity().entityFlavorInternal().dal$temporalContext();
      if (tc != null && ((TemporalSurface) tc).canTick()) {
        throw new IllegalArgumentException("cannot call safeResult on a tickable entity");
      }
      return syncNode.refreshedValue();
    } else {
      throw new IllegalArgumentException("invalid node type : " + node.getClass() + " : " + node);
    }
  }

  public static void witnessVersion(Entity e) {
    TemporalContext tc = e.entityFlavorInternal().dal$temporalContext();
    if (tc != null) tc.witnessVersion(e);
  }

  // called from entityplugin generated argsHash methods for inner entities
  @SuppressWarnings("unused")
  public static int outerHash(Entity e) {
    Object outer = e.$info().outerOrNull(e);
    return (outer == null) ? 1 : outer.hashCode();
  }

  // called from entityplugin generated argsEquals methods for inner entities
  @SuppressWarnings("unused")
  public static boolean outerEquals(Entity a, Entity b) {
    Object outerA = a.$info().outerOrNull(a);
    Object outerB = b.$info().outerOrNull(b);
    return (outerA == null) ? outerB == null : outerA.equals(outerB);
  }

  /**
   * wraps OptimusCancellationException into another OptimusCancellationException so that we capture
   * the stacktrace at this point. Other Throwables are unchanged
   */
  public static Throwable wrapped(Throwable ex) {
    return ex instanceof OptimusCancellationException
        ? ((OptimusCancellationException) ex).wrapped()
        : ex;
  }

  public static <V> AlreadyCompletedPropertyNode<V> acpn(V v, Entity entity, int propertyID) {
    return new AlreadyCompletedPropertyNode<V>(v, entity, NodeTrace.forID(propertyID));
  }

  public static <V> AlreadyCompletedNode<V> acn(V v) {
    return new AlreadyCompletedNode<>(v);
  }

  public static <V> PropertyNode<V> observedValueNode(V v, Entity entity, int propertyID) {
    return CorePluginSupport$.MODULE$.observedValueNode(v, entity, NodeTrace.forID(propertyID));
  }

  public static <V> Node<V> toNode(scala.Function0<V> f) {
    if (f instanceof ITrivialLamdba) return new AlreadyCompletedNode<>(f.apply());
    else if (f instanceof LNodeFunction0<?> lf) return new AsNode0<>((LNodeFunction0<V>) lf);
    else return new AsNodePlain<>(f);
  }

  public static <V> Object mkCompute(Function0<V> compute) {
    if (compute instanceof ITrivialLamdba) return new AlreadyCompletedNode<>(compute.apply());
    return compute;
  }

  public static <V, R> Function1<V, Node<R>> toNodeFactory(Function1<V, R> f) {
    if (f instanceof LNodeFunction1<?, ?>)
      return new AsNodeFactory1<V, R>((LNodeFunction1<V, R>) f);
    else return new AsNodeFactoryPlain1<V, R>(f);
  }

  // Used in $queued functions with @nodeLift params
  public static <V, R> Function1<V, Node<R>> toNodeFQ(Function1<V, Node<R>> f) {
    if (f instanceof LNodeFunction1<?, ?>)
      return new AsNodeFactory1<V, R>((LNodeFunction1<V, R>) f);
    else return f;
  }

  public static <V1, V2, R> Function2<V1, V2, Node<R>> toNodeFactory(Function2<V1, V2, R> f) {
    return new AsNodeFactory2<>((LNodeFunction2<V1, V2, R>) f);
  }

  // Used in $queued functions with @nodeLift params
  public static <V1, V2, R> Function2<V1, V2, Node<R>> toNodeFQ(Function2<V1, V2, Node<R>> f) {
    if (f instanceof LNodeFunction2<?, ?, ?>)
      return new AsNodeFactory2<>((LNodeFunction2<V1, V2, R>) f);
    else return f;
  }
}
