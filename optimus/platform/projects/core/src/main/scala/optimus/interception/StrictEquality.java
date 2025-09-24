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
package optimus.interception;

import java.lang.instrument.UnmodifiableClassException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import optimus.EntityAgent;
import optimus.breadcrumbs.ChainedID;
import optimus.graph.DiagnosticSettings;
import optimus.debug.InstrumentationCmds;

import optimus.graph.Settings;
import scala.math.BigDecimal;
import scala.math.BigInt;
import scala.runtime.BoxesRunTime;
import scala.collection.Seq;
import scala.collection.Set;
import scala.collection.Map;

/*
 * This class is used to toggle a strict equality behaviour in functions that support it.
 * Current usage is to provide strict-equality for @embeddable case classes that override
 * equals. StrictEquality.isActive() is checked in PluginHelpers.strictEquals and the
 * entity plug-in modifies the @embeddable call to ensure that in the case when the toggle
 * is on, PluginHelpers.strictEquals is called.
 */
public class StrictEquality {
  private static final VarHandle vh_activeCount;

  // This is used to indicate that at least one thread has the toggle set
  // In the case that the value is 0, we get to avoid the thread-local check
  // which is more expensive
  @SuppressWarnings("unused") // Updated via VarHandle
  private static int activeCount;

  public static class StrictEqualityFence {
    private int count;

    private void enter() {
      ++count;
    }

    private void exit() {
      --count;
    }
  }

  private static final ThreadLocal<StrictEqualityFence> isActiveForThread =
      ThreadLocal.withInitial(StrictEqualityFence::new);

  public static void ensureLoaded() {
    // This method is a noop but allows  classes that rely on strict equality to ensure our
    // static initializer is run.
  }

  static {
    if (Settings.strictEqualityEnabled) {
      loadInterceptors();
      var lookup = MethodHandles.lookup();
      try {
        vh_activeCount = lookup.findStaticVarHandle(StrictEquality.class, "activeCount", int.class);
        var classes =
            new Class<?>[] {
              BoxesRunTime.class,
              Set.class,
              Seq.class,
              Map.class,
              BigInt.class,
              BigDecimal.class,
              ChainedID.class
            };
        for (Class<?> c : classes) {
          // we are checking for a really far-fetched scenario where the application loads scala
          // these classes explicitly with a different class loader. In this case, we won't be
          // able to re-transform. Since StrictEquality, when enabled, is required for correctness,
          // we want to throw and not continue in such a case.
          if (c.getClassLoader() != ClassLoader.getSystemClassLoader()) {
            throw new InvalidStrictEqualityUsageException(
                "StrictEquality cannot be enabled because the classes that need to be intercepted "
                    + "are not loaded by the System ClassLoader.");
          }
        }
        // Re-transform to cause re-loading so that we can install the interception hooks.
        EntityAgent.retransformOrThrow(classes);
      } catch (UnmodifiableClassException | NoSuchFieldException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    } else {
      vh_activeCount = null;
    }
  }

  private static void loadInterceptors() {
    var interceptEqualsSig =
        "optimus.interception.StrictEquality.interceptEquals(ZLjava/lang/Object;Ljava/lang/Object;)Z";

    // Use the classloader that's being used for scala.
    var clsLoader = scala.runtime.BoxesRunTime.class.getClassLoader();

    // BoxesRunTime.equals considers mathematically equivalent types equal:
    InstrumentationCmds.suffixCallTyped(
        "scala.runtime.BoxesRunTime.equals", interceptEqualsSig, clsLoader);
    // Scala collections implement equal in a way that disregards the concrete type:
    InstrumentationCmds.suffixCallTyped(
        "scala.collection.Set.canEqual", interceptEqualsSig, clsLoader);
    InstrumentationCmds.suffixCallTyped(
        "scala.collection.Seq.canEqual", interceptEqualsSig, clsLoader);
    InstrumentationCmds.suffixCallTyped(
        "scala.collection.Map.canEqual", interceptEqualsSig, clsLoader);
    // scala.math.ScalaNumber types are considered equal if they are mathematically equal
    // We have to specifically intercept these as there is no guarantee that their equals
    // methods will always be called through BoxesRuntime.equals:
    InstrumentationCmds.suffixCallTyped("scala.math.BigInt.equals", interceptEqualsSig, clsLoader);
    // scala.math.BigDecimal needs additional care. The conditions we need to handle are:
    // 1- BigDecimal.equals(Object) will consider int 0 to be equal to BigDecimal(0). This concern
    // is handled the same way as the above cases:
    InstrumentationCmds.suffixCallTyped(
        "scala.math.BigDecimal.equals(Ljava/lang/Object;)Z", interceptEqualsSig, clsLoader);
    // 2- BigDecimal.equals(BigDecimal) will consider BigDecimal("0E7") and BigDecimal(0) equal.
    // even though their underlying java.math.BigDecimal representations won't compare equal.
    // The BigDecimal.equals(Object) implementation will first check if the passed in argument is
    // a BigDecimal and if so call the BigDecimal.equals(BigDecimal) method so we need to also
    // intercept that method.
    // The intercepting function is implemented in ScalaInterceptors where we can refer to scala
    // types (which is forbidden in this class and inside of entityagent in general)
    InstrumentationCmds.suffixCallTyped(
        "scala.math.BigDecimal.equals(Lscala/math/BigDecimal;)Z",
        "optimus.interception.StrictEquality.interceptBigDecimalEquals",
        clsLoader);

    // 3- BigDecimal hashcode is cached and depends on what isDecimalDouble returns. See
    // the comments in the interceptBigDecimalIsDecimalDouble function for more details.
    InstrumentationCmds.suffixCallTyped(
        "scala.math.BigDecimal.isDecimalDouble()Z",
        "optimus.interception.StrictEquality.interceptBigDecimalIsDecimalDouble",
        clsLoader);

    // ChainID equals is implemented to only check for one of the fields which doesn't
    // follow our strict-ness requirements for interning.
    // We'll ensure that if strict equality is active we only consider two ChainedIDs
    // equal if they are reference-equal.
    InstrumentationCmds.suffixCallTyped(
        "optimus.breadcrumbs.ChainedID.equals",
        "optimus.interception.StrictEquality.interceptByRefEquals"
            + "(ZLjava/lang/Object;Ljava/lang/Object;)Z",
        clsLoader);
  }

  public static StrictEqualityFence enter() {
    if (!Settings.strictEqualityEnabled) return null;
    vh_activeCount.getAndAdd(1);
    var fence = isActiveForThread.get();
    fence.enter();
    return fence;
  }

  public static void exit(StrictEqualityFence fence) {
    if (!Settings.strictEqualityEnabled) return;
    var prev = (int) vh_activeCount.getAndAdd(-1);
    if (DiagnosticSettings.schedulerAsserts && (prev <= 0 || fence.count <= 0))
      throw new InvalidStrictEqualityUsageException(
          "StrictEquality was exited more times " + "than entered or exited on the wrong thread");
    fence.exit();
  }

  public static boolean isActive() {
    if (!Settings.strictEqualityEnabled) return false;
    // Only checking the thread-local if there is at least one activation
    // of the toggle to avoid the thread-local overhead
    return ((int) vh_activeCount.getVolatile() > 0 && isActiveForThread.get().count > 0);
  }

  @SuppressWarnings("unused") // Called by entityagent
  public static boolean interceptEquals(boolean result, Object a, Object b) {
    return (result && isActive()) ? a != null && a.getClass() == b.getClass() : result;
  }

  @SuppressWarnings("unused") // Called by entityagent
  public static boolean interceptBigDecimalEquals(boolean result, BigDecimal a, BigDecimal b) {
    // scala.math.BigDecimal will consider BigDecimal("0E7") and BigDecimal(0) equal
    // they have different underlying java.math.BigDecimal representations which won't
    // compare equal. StrictEquality should require that the underlying are equal for
    // the BigDecimals to be considered equal.
    return (result && StrictEquality.isActive()) ? a.underlying().equals(b.underlying()) : result;
  }

  @SuppressWarnings("unused") // Called by entityagent
  public static boolean interceptBigDecimalIsDecimalDouble(boolean result, BigDecimal a) {
    // BigDecimal.hashCode caches the hashcode. The hashcode is dependent on whether
    // isDecimalDouble returns true or not. isDecimalDouble does end up calling equals(BigDecimal)
    // and when StrictEquality is active, we could end up returning false when this function
    // would have normally returned true, thereby ending up with a different hashCode
    // than otherwise. This would end up with us interning a BigDecimal, having considered
    // strictly-equal, to not have the same hashcode as what it is supposed to be equal to
    // in the scala sense. So we need to ensure we maintain the behavior of isDecimalDouble
    // when StrictEquality is active.
    if (!result && StrictEquality.isActive()) {
      var d = a.toDouble();
      return (!java.lang.Double.isInfinite(d) && a.compareTo(BigDecimal.decimal(d)) == 0);
    } else return result;
  }

  @SuppressWarnings("unused") // Called by entityagent
  public static boolean interceptByRefEquals(boolean result, Object a, Object b) {
    return (result && StrictEquality.isActive()) ? a == b : result;
  }
}
