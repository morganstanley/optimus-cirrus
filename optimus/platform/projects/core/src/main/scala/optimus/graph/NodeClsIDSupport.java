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

import static java.lang.invoke.MethodHandles.filterArguments;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodHandles.permuteArguments;
import static java.lang.invoke.MethodType.methodType;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.runtime.ObjectMethods;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import optimus.debug.RTVerifierCategory;
import optimus.graph.diagnostics.rtverifier.RTVerifierReporter$;

/**
 * Provides reflective value-based equality and hashCode for objects which implement NodeClsID
 * (based on the class and all of the fields)
 *
 * <p>Caches reflective access to captured fields. A number of nodes are not property nodes but need
 * be compared and hashed. Consider:
 *
 * <pre>{@code
 * int a = ...
 * var b = SomeObject()
 * int c = ...
 * Tweak.byName(e.x: = a + b.z * c) }// Captures all those values
 * </pre>
 *
 * In order to decide that the tweak is the same we need to compare and hash those captured values
 *
 * <p>In order not to generate all the code for args()/equals()/hashCode on every node we pay the
 * price lazily once to generate this FieldMap and update the previously generated
 *
 * <pre>{@code
 * static field __nclsID;
 * optimus.graph.NodeClsID#getClsID() { return __nclsID; }
 * }</pre>
 *
 * <p>First time __nclsID is 0 (uninitialized) and will be updated via reflection here.
 */
public class NodeClsIDSupport {
  private static final MethodType mt_equals = methodType(boolean.class, Object.class, Object.class);
  private static final MethodType mt_hashCode = methodType(int.class, Object.class);
  private static final MethodHandle mh_identity;
  private static final String idFieldName = "__nclsID";
  private static final Object[] emptyArgs = new Object[0];

  public static class FieldMap {
    final MethodHandle equal;
    final MethodHandle hash;
    final MethodHandle args;
    final int clsHash;

    public FieldMap(MethodHandle equals, MethodHandle hash, MethodHandle args, int clsHash) {
      this.equal = equals;
      this.hash = hash;
      this.args = args;
      this.clsHash = clsHash;
    }

    public int hashCode(Object a) throws Throwable {
      if (hash == null) return clsHash;
      return (int) hash.invokeExact(a) * 37 + clsHash;
    }

    public boolean equals(Object a, Object b) throws Throwable {
      if (equal == null) return true;
      return (boolean) equal.invokeExact(a, b);
    }

    public Object[] argsOf(NodeTask a) throws Throwable {
      if (args == null) return emptyArgs;
      return (Object[]) args.invokeExact((Object) a);
    }
  }

  private static final ArrayList<FieldMap> _fmap = new ArrayList<>();
  private static final ConcurrentHashMap<Class<?>, FieldMap> _cls2fmap = new ConcurrentHashMap<>();

  static {
    _fmap.add(null); // Reserve field offset

    var identity_mt = methodType(Object[].class, Object[].class);
    try {
      mh_identity = lookup().findStatic(NodeClsIDSupport.class, "identity", identity_mt);
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private static int setupFieldMapAndClsID(Object a) throws Throwable {
    var r = createFieldMapAndClsIdHandle(a.getClass(), /* includeOuter = */ true);
    FieldMap fm = r.getKey();
    int id;
    synchronized (_fmap) {
      id = ((NodeClsID) a).getClsID(); // re-read id
      if (id == 0) {
        _fmap.add(fm);

        id = _fmap.size() - 1;
        var nclsIDHandle = r.getValue();
        nclsIDHandle.setVolatile(id); // Now un-synchronized code can read the values of FieldMap
      }
    }
    return id;
  }

  private static final boolean rtVerifierEnabled = DiagnosticSettings.enableRTVerifier;
  private static final boolean dumpCapturedSM = Settings.dumpCapturedStateMachine;

  private static FieldMap createFieldMap(Class<?> cls) {
    return createFieldMap(cls, true);
  }

  private static FieldMap createFieldMap(Class<?> cls, boolean includeOuter) {
    try {
      return createFieldMapAndClsIdHandle(cls, includeOuter).getKey();
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("StatementWithEmptyBody")
  private static HashMap.SimpleEntry<FieldMap, VarHandle> createFieldMapAndClsIdHandle(
      Class<?> cls, boolean includeOuter) throws Throwable {
    Field[] fields = cls.getDeclaredFields();
    VarHandle nclsIDfield = null; // We will need it to store back the newly allocated slot
    var activeFields = new ArrayList<MethodHandle>();

    var lookup = MethodHandles.privateLookupIn(cls, lookup());
    for (Field f : fields) {
      if (Modifier.isStatic(f.getModifiers())) {
        if (f.getName().equals(idFieldName)) nclsIDfield = lookup.unreflectVarHandle(f);
        // otherwise we will skip the static field
      } else if (!includeOuter && f.getName().equals("$outer")) {
        // skip this one for PropertyNodes; their equality already compares the entity (and does it
        // better)
      } else {
        Class<?> type = f.getType();
        if ((rtVerifierEnabled || dumpCapturedSM)
            && NodeStateMachineBase.class.isAssignableFrom(type)) {
          reportRTVerifier(cls);
        }
        activeFields.add(lookup.unreflectGetter(f));
      }
    }

    // It's desirable (though not strictly required) for objects of different classes to have
    // different hashCodes, and for equal objects to have the same hashCode between processes.
    // This seems like a good enough option:
    int clsHashCode = cls.getName().hashCode();
    if (activeFields.isEmpty())
      return new HashMap.SimpleEntry<>(new FieldMap(null, null, null, clsHashCode), nclsIDfield);

    var getters = activeFields.toArray(new MethodHandle[0]);
    var h_equals = objectMethod("equals", mt_equals, cls, getters);
    var h_hashCode = objectMethod("hashCode", mt_hashCode, cls, getters);
    var h_args = fieldsAsArray(cls, getters);

    var fm = new FieldMap(h_equals, h_hashCode, h_args, clsHashCode);
    return new HashMap.SimpleEntry<>(fm, nclsIDfield);
  }

  static Object[] identity(Object... values) {
    return values;
  }

  private static MethodHandle fieldsAsArray(Class<?> cls, MethodHandle[] getters) {
    var objGetters = new MethodHandle[getters.length];
    for (int i = 0; i < getters.length; i++)
      objGetters[i] = getters[i].asType(methodType(Object.class, cls));

    var collector = mh_identity.asCollector(Object[].class, getters.length);
    var fieldsCollector = filterArguments(collector, 0, objGetters);
    var indexes = new int[getters.length];
    var typed = permuteArguments(fieldsCollector, methodType(Object[].class, cls), indexes);
    return typed.asType(methodType(Object[].class, Object.class));
  }

  private static MethodHandle objectMethod(
      String name, MethodType mType, Class<?> cls, MethodHandle[] getters) throws Throwable {
    var lookup = lookup();
    var mhCls = MethodHandle.class; // Special argument to return MethodHandle
    var typedMethod = ObjectMethods.bootstrap(lookup, name, mhCls, cls, "a;b;c", getters);
    return ((MethodHandle) typedMethod).asType(mType);
  }

  private static void reportRTVerifier(Class<?> cls) {
    var location = SourceLocator.sourceOf(cls);
    var sw = new StringWriter();
    sw.write(
        "NodeStateMachine should not be used for hashing/comparison. \n"
            + "It has been captured by mistake, often by defining it in @node\n"
            + "Please consider refactoring! ("
            + location
            + ")\n");
    new Exception().printStackTrace(new PrintWriter(sw));
    var details = sw.toString();
    if (dumpCapturedSM) System.err.println(details);
    if (rtVerifierEnabled) {
      var category = RTVerifierCategory.NODE_WITH_CAPTURED_FSM;
      RTVerifierReporter$.MODULE$.reportClassViolation(category, location, details, cls.getName());
    }
  }

  /** This code only works for PropertyNodes because the caller explicitly compares the entity */
  public static boolean equals(NodeKey<?> a, NodeKey<?> b) throws Throwable {
    if (a == b) return true;
    if (a == null || b == null) return false;
    // Class equality check is ok here because proxies compare via their srcNodeTemplates
    // [SEE_NO_SYMMETRY_EQUALS_FOR_CACHING]
    if (a.getClass() != b.getClass()) return false;
    FieldMap nclsID = a.propertyInfo().fieldMap;
    if (nclsID == null)
      a.propertyInfo().fieldMap = nclsID = createFieldMap(a.getClass(), /* includeOuter = */ false);

    return nclsID.equals(a, b);
  }

  /** This code only works for PropertyNodes because the caller explicitly compares the entity */
  public static int hashCode(NodeKey<?> a) throws Throwable {
    FieldMap fm = a.propertyInfo().fieldMap;
    if (fm == null)
      a.propertyInfo().fieldMap = fm = createFieldMap(a.getClass(), /* includeOuter = */ false);
    return fm.hashCode(a);
  }

  public static boolean equals(Object a, Object b) throws Throwable {
    if (a == b) return true;
    if (a instanceof NodeClsID clsID) {
      if (a.getClass() != b.getClass()) return false;
      int id = clsID.getClsID();
      if (id == 0) id = setupFieldMapAndClsID(a);
      return _fmap.get(id).equals(a, b);
    } else if (a == null) return false;
    else return a.equals(b);
  }

  public static int hashCode(Object a) throws Throwable {
    if (a instanceof NodeClsID clsID) {
      int id = clsID.getClsID();
      if (id == 0) id = setupFieldMapAndClsID(a);
      return _fmap.get(id).hashCode(a);
    } else return a.hashCode();
  }

  public static boolean equalsUsingClassBasedLookup(Object a, Object b) throws Throwable {
    if (a == b) return true;
    if (a == null || b == null || a.getClass() != b.getClass()) return false;
    return getFieldMapByClass(a).equals(a, b);
  }

  public static int hashCodeUsingClassBasedLookup(Object a) throws Throwable {
    if (a == null) return 0;
    return getFieldMapByClass(a).hashCode(a);
  }

  private static FieldMap getFieldMapByClass(Object a) {
    return _cls2fmap.computeIfAbsent(a.getClass(), NodeClsIDSupport::createFieldMap);
  }

  /* Node the order of return arguments in not stable with JVM restarts! */
  public static Object[] getArgs(NodeTask a) {
    if (a == null) return null;
    try {
      FieldMap fm = getFieldMapByClass(a);
      return fm.argsOf(a);
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }
}
