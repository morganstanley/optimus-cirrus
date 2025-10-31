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

import java.lang.invoke.VarHandle;
import java.util.Objects;

import optimus.graph.AlreadyCompletedNode;
import optimus.graph.AlreadyCompletedPropertyNode;
import optimus.graph.CorePluginSupport$;
import optimus.graph.Node;
import optimus.graph.NodeFuture;
import optimus.graph.NodeTrace;
import optimus.graph.OptimusCancellationException;
import optimus.graph.PropertyInfo;
import optimus.graph.PropertyNode;
import optimus.graph.loom.*;
import optimus.interception.StrictEquality;
import optimus.platform.pickling.AlreadyUnpickledPropertyNode;
import optimus.platform.pickling.ContainsUnresolvedReference;
import optimus.platform.pickling.LazyPickledReference;
import optimus.platform.pickling.MaybePickledReference;
import optimus.platform.storable.Entity;
import optimus.platform.storable.EntityImpl;
import optimus.platform.storable.ModuleEntityToken;
import optimus.platform.storable.NonDALEntityFlavor;
import optimus.platform.storable.Storable;
import optimus.platform.pickling.UnresolvedItemWrapper;
import scala.Function0;
import scala.Function1;
import scala.Function2;
import scala.Function3;
import scala.Product;

@SuppressWarnings("unchecked")
public class PluginHelpers {

  // Called from plugin-generated code
  @SuppressWarnings("unused")
  public static int hashOf(int prev, boolean v) {
    return prev * 31 + (v ? 1231 : 1237);
  }

  // Called from plugin-generated code
  @SuppressWarnings("unused")
  public static int hashOf(int prev, byte v) {
    return prev * 31 + v;
  }

  // Called from plugin-generated code
  @SuppressWarnings("unused")
  public static int hashOf(int prev, short v) {
    return prev * 31 + v;
  }

  // Called from plugin-generated code
  @SuppressWarnings("unused")
  public static int hashOf(int prev, char v) {
    return prev * 31 + v;
  }

  // Called from plugin-generated code
  @SuppressWarnings("unused")
  public static int hashOf(int prev, int v) {
    return prev * 31 + v;
  }

  // Called from plugin-generated code
  @SuppressWarnings("unused")
  public static int hashOf(int prev, long v) {
    return prev * 31 + Long.hashCode(v);
  }

  // Called from plugin-generated code
  @SuppressWarnings("unused")
  public static int hashOf(int prev, double v) {
    return prev * 31 + Double.hashCode(v);
  }

  // Called from plugin-generated code
  @SuppressWarnings("unused")
  public static int hashOf(int prev, float v) {
    return prev * 31 + Float.floatToIntBits(v);
  }

  // Called from plugin-generated code
  @SuppressWarnings("unused")
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
  // Called from plugin-generated code
  @SuppressWarnings("unused")
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
  // Called from plugin-generated code
  @SuppressWarnings("unused")
  public static boolean equalsAvoidingZero(int hash1, int hash2) {
    return hash1 == hash2 || hash1 == 0 || hash2 == 0;
  }

  /* use DoubleCompareBenchmark when considering other implementations
   * Note - equals(Float.NaN, Float.NaN) == true */
  // Called from plugin-generated code
  @SuppressWarnings("unused")
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
  // Called from plugin-generated code
  @SuppressWarnings("unused")
  public static boolean canEqualStorable(Storable p1, Storable p2) {
    if (p1 == p2) return true;
    if (p1 == null || p2 == null) return false;
    // fix this to be Storable - cache the hashcode
    if (p1 instanceof Entity)
      return p1.getClass() == p2.getClass() && p1.hashCode() == p2.hashCode();
    return p1.getClass() == p2.getClass();
  }

  // Called from plugin-generated code
  @SuppressWarnings("unused")
  public static Object safeResult(Entity e, VarHandle vh) {
    // safeResult should only be getting called from plugin-generated
    // argsHash and argsEquals methods, which in turn should only
    // be getting called for non-DALEntityFlavor entities. For DAL
    // entities, the equal/hashcode is based on their DAL identity, not their
    // args.
    assert (e.entityFlavorInternal() instanceof NonDALEntityFlavor);
    // the value is always stable for heap entities so we can just
    // get here rather than getVolatile.
    return vh.get(e);
  }

  // This method is used to ensure that @embeddable case classes can be interned safely.
  // The requirement for it stems from existing code that have defined custom equality methods
  // that don't necessarily adhere to strict equality. When the plugin sees such classes, it will
  // generate code that calls this method when StrictEquality.isActive() is true.
  @SuppressWarnings("unused")
  public static boolean strictEquals(Product inst, Object that) {
    if (inst == that) return true;
    // plugin sets inst to be the "this" of the embeddable so no check
    // for inst == null here:
    if (that == null || that.getClass() != inst.getClass()) return false;
    var other = (Product) that;
    for (int i = 0; i < inst.productArity(); ++i) {
      if (!Objects.equals(inst.productElement(i), other.productElement(i))) return false;
    }
    return true;
  }

  // Called from plugin-generated code
  @SuppressWarnings("unused")
  public static boolean strictEqualsIsActive() {
    return StrictEquality.isActive();
  }

  // Called from plugin-generated code
  @SuppressWarnings("unused")
  public static <T> T resolveRef(Entity e, VarHandle vh, PropertyInfo<?> propInfo) {
    var v = vh.getVolatile(e);
    if (!(v instanceof ContainsUnresolvedReference)) {
      // A direct entity reference that's already resolved.
      if (!propInfo.isDirectlyTweakable()) return (T) v;
      else return (T) new AlreadyUnpickledPropertyNode<>(v, e, propInfo).lookupAndGet();
    } else {
      return (T) resolveRefNewNode(e, vh, propInfo).lookupAndGet();
    }
  }

  // Called from plugin-generated code
  @SuppressWarnings("unused")
  public static <T> NodeFuture<T> resolveRefQueued(
      Entity e, VarHandle vh, PropertyInfo<?> propInfo) {
    var v = vh.getVolatile(e);
    if (propInfo.isDirectlyTweakable())
      return (NodeFuture<T>) resolveRefNewNode(e, vh, propInfo).lookupAndEnqueue();
    else if (v instanceof EntityImpl ent) return (NodeFuture<T>) ent;
    else if (!(v instanceof ContainsUnresolvedReference))
      return (NodeFuture<T>) new AlreadyCompletedNode<>(v);
    else return (NodeFuture<T>) resolveRefNewNode(e, vh, propInfo).lookupAndEnqueue();
  }

  // Called from plugin-generated code
  @SuppressWarnings("unused")
  public static <T> PropertyNode<T> resolveRefNewNode(
      Entity e, VarHandle vh, PropertyInfo<?> propInfo) {
    Object v;
    // We need to make sure that in all circumstances, the PropertyNode types that we
    // return from this function are always extending MaybePickledReference<T>. There currently
    // is code (e.g. optimus.graph.PluginSupport.getEntityReference) which type-matches on this
    // for specific logic. We let the compiler help us by declaring our val as
    // MaybePickledReference<T> but cast it to PropertyNode<T> at the end.
    MaybePickledReference<?> res;
    while (true) {
      v = vh.getVolatile(e);
      if (!(v instanceof ContainsUnresolvedReference)) {
        // A structure that contains entity references that is
        // already resolved
        res = new AlreadyUnpickledPropertyNode<>((T) v, e, propInfo);
        break;
      } else if (v instanceof LazyPickledReference<?> lpr) {
        // LPR in flight
        res = lpr;
        break;
      } else if (v instanceof ModuleEntityToken) {
        // This is a simple case, the resolution is not async so just
        // resolve and set it to res, and let the loop wrap it in
        // an ACN in the next iteration
        var module = ((ModuleEntityToken) v).resolve();
        if (vh.compareAndSet(e, v, module)) {
          res = new AlreadyUnpickledPropertyNode<>(module, e, propInfo);
          break;
        }
      } else if (v instanceof UnresolvedItemWrapper<?> wrapper) {
        // A type that contains an unresolved reference that's been wrapped. We have to wrap
        // types that don't extend ContainsUnresolvedReference in a wrapper so that we know
        // they are unresolved and need resolving.
        // we'll create the LPR, and not terminate the loop so we go through the loop again and go
        // into the LPR case.
        var lpr =
            new LazyPickledReference<>(wrapper.unresolved(), propInfo.unpickler(), e, propInfo, vh);
        if (vh.compareAndSet(e, v, lpr)) {
          res = lpr;
          break;
        }
      } else {
        // A type that contains an unresolved reference that is not wrapped because we are able
        // to extend ContainsUnresolvedReference directly on the type that has the pickled
        // representation (e.g. EntityReference or SlottedBufferAs*)
        var lpr = new LazyPickledReference<>(v, propInfo.unpickler(), e, propInfo, vh);
        if (vh.compareAndSet(e, v, lpr)) {
          res = lpr;
          break;
        }
      }
    }
    return (PropertyNode<T>) (Object) res; // Intellij inspection does not like the cast.
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
    return Objects.equals(outerA, outerB);
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
    return new AlreadyCompletedPropertyNode<>(v, entity, NodeTrace.forID(propertyID));
  }

  public static <V> AlreadyCompletedNode<V> acn(V v) {
    return new AlreadyCompletedNode<>(v);
  }

  public static <V> PropertyNode<V> observedValueNode(V v, Entity entity, int propertyID) {
    return CorePluginSupport$.MODULE$.observedValueNode(v, entity, NodeTrace.forID(propertyID));
  }

  public static <V> Node<V> toNode(scala.Function0<V> f) {
    if (f instanceof TrivialNode) return new AlreadyCompletedNode<>(f.apply());
    else if (f instanceof LNodeFunction0<?> lf) return new AsNode0<>((LNodeFunction0<V>) lf);
    else return new AsNodePlain<>(f);
  }

  public static <V> Object mkCompute(Function0<V> compute) {
    if (compute instanceof TrivialNode) return new AlreadyCompletedNode<>(compute.apply());
    return compute;
  }

  public static <V, R> Function1<V, Node<R>> toNodeFactory(Function1<V, R> f) {
    if (f instanceof LNodeFunction1<?, ?>) return new AsNodeFactory1<>((LNodeFunction1<V, R>) f);
    return new AsNodeFactoryPlain1<>(f);
  }

  // Used in $queued functions with @nodeLift params
  public static <V, R> Function1<V, Node<R>> toNodeFQ(Function1<V, Node<R>> f) {
    if (f instanceof LNodeFunction1<?, ?>) return new AsNodeFactory1<>((LNodeFunction1<V, R>) f);
    return f;
  }

  public static <V1, V2, R> Function2<V1, V2, Node<R>> toNodeFactory(Function2<V1, V2, R> f) {
    return new AsNodeFactory2<>((LNodeFunction2<V1, V2, R>) f);
  }

  // Used in $queued functions with @nodeLift params
  public static <V1, V2, R> Function2<V1, V2, Node<R>> toNodeFQ(Function2<V1, V2, Node<R>> f) {
    if (f instanceof LNodeFunction2<?, ?, ?>)
      return new AsNodeFactory2<>((LNodeFunction2<V1, V2, R>) f);
    return f;
  }

  public static <V1, V2, V3, R> Function3<V1, V2, V3, Node<R>> toNodeFactory(
      Function3<V1, V2, V3, R> f) {
    return new AsNodeFactory3<>((LNodeFunction3<V1, V2, V3, R>) f);
  }

  public static <R> NodeFunction0<R> toNodeF(Function0<R> f) {
    return (NodeFunction0<R>) f;
  }

  // marker to enable debugging mode for loom
  @SuppressWarnings("unused") // injectable by on demand in code and detected by LoomAdapter
  public static void enableCompilerDebug() {}

  // marker to disable code motion for loom
  @SuppressWarnings("unused") // injectable by on demand in code and detected by LoomAdapter
  public static void setCompilerLevelZero() {}
}
