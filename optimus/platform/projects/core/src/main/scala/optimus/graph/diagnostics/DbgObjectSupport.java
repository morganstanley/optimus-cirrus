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
package optimus.graph.diagnostics;

import static java.lang.invoke.MethodType.methodType;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is very similar to NodeClsIDSupport
 *
 * <p>Note: NodeClsIDSupport does not check for inherited fields! This is because it specifically
 * doesn't want them! PropertyNode or NodeFunction for example does need to compare internal fields
 * of the super classes
 *
 * <p>Here we want to compare all non-static fields
 */
public class DbgObjectSupport {
  private static final MethodType objectGetterType = methodType(Object.class, Objects.class);

  public static class FieldMap {
    FieldAccess[] fields;

    FieldMap(FieldAccess[] fieldAccesses) {
      this.fields = fieldAccesses;
    }
  }

  static class FieldAccess {
    public Field field; // Store original j.l.r.Field to print out names etc...
    public MethodHandle get_mh;
    public boolean reported; // Optionally report field once

    public FieldAccess(Field f, MethodHandle getter) {
      this.field = f;
      this.get_mh = getter;
    }

    public boolean equalsTyped(Object a, Object b) throws Throwable {
      Object fa = get_mh.invokeExact(a);
      Object fb = get_mh.invokeExact(b);
      return Objects.equals(fa, fb);
    }
  }

  private static void setUp(Class<?> cls, Class<?> stopAtCls, ArrayList<FieldAccess> fas)
      throws IllegalAccessException {
    Field[] fields = cls.getDeclaredFields();
    var lookup = MethodHandles.lookup();
    for (Field f : fields) {
      if (!Modifier.isStatic(f.getModifiers())) {
        f.setAccessible(true);
        fas.add(new FieldAccess(f, lookup.unreflectGetter(f).asType(objectGetterType)));
      }
    }
    if (cls.getSuperclass() != stopAtCls) setUp(cls.getSuperclass(), stopAtCls, fas);
  }

  public static FieldMap setUp(Class<?> cls, Class<?> stopAtCls) throws IllegalAccessException {
    var fas = new ArrayList<FieldAccess>();
    setUp(cls, stopAtCls, fas);
    return new FieldMap(fas.toArray(new FieldAccess[0]));
  }

  private static final AtomicInteger msgCount = new AtomicInteger();

  private static void expectAllToEqual(FieldMap fmap, Object a, Object b) throws Throwable {
    for (var fa : fmap.fields) {
      if (!fa.reported && !fa.equalsTyped(a, b)) {
        fa.reported = true;
        System.err.println(
            "WARNING: Difference (" + msgCount.incrementAndGet() + "): " + fa.field.toString());
      }
    }
  }

  private static class VisitedMap extends IdentityHashMap<Object, Object> {
    int recurseCount;
  }

  private static final ThreadLocal<VisitedMap> entityInEquals =
      ThreadLocal.withInitial(VisitedMap::new);

  public static void expectAllToEqualsSafe(FieldMap fmap, Object a, Object b) throws Throwable {
    var hash = entityInEquals.get();
    var prevValue = hash.putIfAbsent(a, b);
    hash.recurseCount++;
    try {
      if (prevValue == null) expectAllToEqual(fmap, a, b);
    } finally {
      hash.recurseCount--;
      if (hash.recurseCount == 0) hash.clear();
    }
  }
}
