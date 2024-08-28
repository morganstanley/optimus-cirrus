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
package optimus.graph;

import java.nio.ByteBuffer;

import java.util.function.Predicate;
import java.util.regex.Pattern;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.ptr.ByReference;
import com.sun.jna.ptr.LongByReference;
import optimus.debug.InstrumentationConfig;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;
import com.sun.jna.ptr.IntByReference;

public class NativeMemoryUtils {
  private interface JEMalloc extends Library {
    int mallctl(String name, ByReference oldp, IntByReference lenp, ByReference newp, long newlen);
  }

  private static final JEMalloc jemalloc;

  static {
    GCNative.ensureLoaded();
    if (DiagnosticSettings.captureNativeAllocations) {
      try {
        jemalloc = Native.load("jemalloc", JEMalloc.class);
      } catch (UnsatisfiedLinkError e) {
        var logger = LoggerFactory.getLogger(NativeMemoryUtils.class);
        logger.error("Native allocation capture requires jemalloc", e);
        throw e;
      }
    } else {
      jemalloc = null;
    }
  }

  /**
   * Allocate a ByteBuffer and return the jemalloc allocated size, for diagnostic reasons.
   *
   * @param bufferSize
   * @return allocatedSize, which should be about the same as bufferSize
   */
  public static long sizeOfByteBuffer(int bufferSize) {
    if (!DiagnosticSettings.captureNativeAllocations) return -1;
    var m1 = threadAllocated();
    ByteBuffer.allocateDirect(bufferSize);
    var m2 = threadAllocated();
    return m2 - m1;
  }

  public static void selfCheck() {
    var s = sizeOfByteBuffer(5000);
    if (s > 4000) return;

    throw new IllegalStateException(
        "Failed self-check for jemalloc: allocated buffer of size 5000"
            + " but jemalloc reported a size of "
            + s
            + ". Does jeMalloc come before other allocators on your LD_PRELOAD path?");
  }

  private static final Unsafe U = UnsafeAccess.getUnsafe();

  private static class AllocationPointers {
    long snapped;
    long allocatedPtr;
    long freedPtr;

    @SuppressWarnings("unused")
    public long getSnapped() {
      return snapped;
    }

    AllocationPointers() {
      var addrRef = new LongByReference();
      var lenRef = new IntByReference();
      lenRef.setValue(8);

      var ret = jemalloc.mallctl("thread.allocatedp", addrRef, lenRef, null, 0);
      assert ret == 0 : "Unexpected return code " + ret + "from mallctl";
      allocatedPtr = addrRef.getValue();
      ret = jemalloc.mallctl("thread.deallocatedp", addrRef, lenRef, null, 0);
      assert ret == 0 : "Unexpected return code " + ret + "from mallctl";
      freedPtr = addrRef.getValue();
    }

    public long used() {
      if (DiagnosticSettings.captureNativeAllocations)
        return U.getLong(allocatedPtr) - U.getLong(freedPtr);
      else return 0;
    }

    public void snap() {
      snapped = used();
    }

    public long usedSinceSnap() {
      long u = used() - snapped;
      return u;
    }
  }

  private static ThreadLocal<AllocationPointers> tlPointers =
      new ThreadLocal<>() {
        @Override
        protected AllocationPointers initialValue() {
          return new AllocationPointers();
        }
      };

  @SuppressWarnings("unused")
  public static long threadAllocated() {
    if (DiagnosticSettings.captureNativeAllocations) return tlPointers.get().used();
    else return 0;
    //
  }

  @SuppressWarnings("unused")
  public static void snap() {
    tlPointers.get().snap();
  }

  @SuppressWarnings("unused")
  public static long threadAllocatedSinceSnap() {
    return tlPointers.get().usedSinceSnap();
  }

  public static class MemoryTracker {
    public long __memory = 0L;
    public Byte[] __waste = null;
  }

  // "a C programmer can write C in any language"
  private static ThreadLocal<int[]> callDepth = ThreadLocal.withInitial(() -> new int[] {0});

  private static int enter() {
    return callDepth.get()[0]++;
  }

  private static int exit() {
    return --callDepth.get()[0];
  }

  public static long startRecording() {
    if (enter() == 0) {
      return threadAllocated();
    } else return 0;
  }

  public static void setMemory(Object obj, long initial) {
    if (exit() > 0) return;
    if (obj instanceof MemoryTracker) {
      var mem = threadAllocated() - initial;
      var mt = (MemoryTracker) obj;
      if (mt.__memory != 0) return;
      mt.__memory = mem;
      if (DiagnosticSettings.duplicateNativeAllocations && mem > 0)
        mt.__waste = new Byte[(int) mem];
    }
  }

  public static void onFail(Throwable f, long initial) {
    exit();
  }

  public static long getMemory(Object obj) {
    if (obj instanceof MemoryTracker) return ((MemoryTracker) obj).__memory;
    else return -1;
  }

  // Matches a signature with something other than a single-letter (i.e. basic java) return type.
  // Non-boring methods found in JNI wrapper packages have a high probability of returning JNI
  // wrapper objects.
  private static final Predicate<String> notBoring =
      Pattern.compile(".*\\)(\\w)").asMatchPredicate().negate();

  private static final String CLS = "optimus/graph/NativeMemoryUtils";

  private static boolean _ranSelfCheck = false;

  public static synchronized void enable(Object id, Predicate<String> classPredicate) {
    if (DiagnosticSettings.captureNativeAllocations) {
      // We have to wait until NativeMemoryUtils is fully loaded to do the self-check, so we don't
      // do it on cinit. Instead, we do it here when we first attempt to use the library. We only
      // throw once though.
      if (!_ranSelfCheck) {
        _ranSelfCheck = true;
        selfCheck();
      }

      InstrumentationConfig.addAllMethodPatchAndChangeSuper(
          id,
          classPredicate,
          (desc, ignored) -> notBoring.test(desc),
          "optimus/graph/NativeMemoryUtils$MemoryTracker",
          new InstrumentationConfig.MethodRef(CLS, "startRecording", "()J"),
          new InstrumentationConfig.MethodRef(CLS, "setMemory"),
          new InstrumentationConfig.MethodRef(CLS, "onFail"));
    }
  }

  // E.g. enable("myjni.package")
  public static void enable(String prefix) {
    enable(prefix, c -> c.startsWith(prefix));
  }
}
