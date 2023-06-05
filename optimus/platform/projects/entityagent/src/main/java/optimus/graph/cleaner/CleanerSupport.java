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
package optimus.graph.cleaner;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.ref.Cleaner;
import java.util.ArrayList;

public final class CleanerSupport {

  private static class MethodRef {
    String owner, name;
    MethodHandle mh; // Lazily found as we need to be in the right class loader

    MethodRef(String owner, String name) {
      this.owner = owner;
      this.name = name;
    }
  }

  private static final ArrayList<MethodRef> methodRefs = new ArrayList<>();
  private static final Cleaner cleaner = Cleaner.create();

  private static class State implements Runnable {
    private final int id;
    private final long pointer;

    State(int id, long pointer) {
      this.id = id;
      this.pointer = pointer;
    }

    private static final MethodType methodType = MethodType.methodType(void.class, long.class);
    @Override public void run() {
      try {
        var methodRef = methodRefs.get(id);
        var mh = methodRef.mh;
        if (mh == null) {
          var cls = Class.forName(methodRef.owner);
          mh = methodRef.mh = MethodHandles.lookup().findStatic(cls, methodRef.name, methodType);
        }
        mh.invokeExact(pointer);
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static Cleaner.Cleanable register(Object instance, long pointer, boolean ownership, int callSiteId) {
    // we don't register a cleanable if we do not own the instance
    if (!ownership) return null;
    return cleaner.register(instance, new  State(callSiteId, pointer));
  }

  public static int registerCallSiteAtCompile(String deleteCls, String deleteMethod, int suggestedID) {
    var id = suggestedID;
    var mr = new MethodRef(deleteCls, deleteMethod);
    if (suggestedID < 0) {
      synchronized (methodRefs) {
        id = methodRefs.size();
        methodRefs.add(mr);
      }
    } else
      methodRefs.set(id, mr);
    return id;
  }

  public static int reserveCallSiteAtCompile(int suggestedID) {
    var id = suggestedID;
    if (suggestedID < 0) {
      synchronized (methodRefs) {
        id = methodRefs.size();
        methodRefs.add(null);
      }
    }
    return id;
  }

}