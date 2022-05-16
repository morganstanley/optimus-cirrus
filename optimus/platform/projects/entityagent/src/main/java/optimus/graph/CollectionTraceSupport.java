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

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public class CollectionTraceSupport {
  private final static ConcurrentHashMap<String, LongAdder> statistics = new ConcurrentHashMap<>();
  private final static ConcurrentHashMap<String, LongAdder> callerStatistics = new ConcurrentHashMap<>();
  private final static ConcurrentHashMap<String, LongAdder> ctorStatistics = new ConcurrentHashMap<>();

  /**
   * This method will be injected for the collection methods with parameter, something like:
   * class HashMap extends ... {
   *   def get(key: A): Option[B] = {
   *     val params = new Object[] { key }
   *     CollectionTraceSupport.traceInvokedWithParam(this, "pgk/HashMap", "get", "(A)Option", params)
   *     ...
   *   }
   * }
   * @param colIns  collection instance
   * @param colCls  collection declare type name
   * @param method  collection method name
   * @param desc    collection method description (type signature - without generic info)
   * @param params  collection method parameters
   */
  public static void traceInvokedWithParam(Object colIns, String colCls, String method, String desc, Object[] params) {
    String key = colIns.getClass().getName() + "::" + method + desc;
    traceInvokedImpl(statistics, key);
  }

  /**
   * N.B. Because $init$ and <clinit> are static methods as well, so we will generate this call in them
   *
   * This method will be injected for the static collection methods with parameter, something like:
   * object HashMap  {
   *   private def liftMerger[A1, B1](mergef: MergeFunction[A1, B1]): Merger[A1, B1] = {
   *     val params = new Object[] { mergef }
   *     CollectionTraceSupport.traceInvokedWithParam(this, "pgk/HashMap", "liftMerger", "(MergeFunction;)Merge", params)
   *     ...
   *   }
   * }
   * @param colCls  collection declare type name
   * @param method  collection method name
   * @param desc    collection method description (type signature - without generic info)
   * @param params  collection method parameters
   */
  public static void traceStaticInvokedWithParam(String colCls, String method, String desc, Object[] params) {
    String key = colCls + "::" + method + desc;

    // The $init$ and <clinit> methods are static, so we inject the static invoke method
    if (method.equals("$init$") || method.equals("<clinit>")) traceInvokedImpl(ctorStatistics, key);
    else traceInvokedImpl(statistics, key);
  }

  /**
   * This method will be injected for the collection constructor with parameter, something like:
   * class StringBuilder (
   *   def <init>(str: String) = {
   *     super(...)
   *     val params = new Object[] { str }
   *     CollectionTraceSupport.traceCtorInvokedWithParam(this, "pkg/StringBuilder", "<init>", "(S)V", params)
   *   }
   * }
   * @param colIns  collection instance
   * @param colCls  collection declare type name
   * @param method  collection method name
   * @param desc    collection method description (type signature - without generic info)
   * @param params  collection method parameters
   */
  public static void traceCtorInvokedWithParam(Object colIns, String colCls, String method, String desc, Object[] params) {
    String key = colIns.getClass().getName() + "::" + method + desc;

    traceInvokedImpl(ctorStatistics, key);
  }

  /**
   * This method will be injected for the collection constructor without parameter, something like:
   * class StringBuilder {
   *   def <init>() = {
   *      super(...)
   *      CollectionTraceSupport.traceCtorInvokec(this, "pkg/StringBuilder", "<init>", "()V")
   *   }
   * }
   * @param colIns  collection instance
   * @param colCls  collection declare type name
   * @param method  collection method name
   * @param desc    collection method description (type signature - without generic info)
   */
  public static void traceCtorInvoked(Object colIns, String colCls, String method, String desc) {
    traceCtorInvokedWithParam(colIns, colCls, method, desc, null);
  }

  /**
   * This method will be injected into Scala collection methods with no parameter list as:
   * class HashMap extends ... {
   *   override def toList() = {
   *     CollectionTrace.traceInvoked(this, "pkg/HashMap", "toList", "()List")
   *     ...
   *   }
   * }
   * @param colIns  collection instance (concrete collection instance)
   * @param colCls  collection class name (where the method is defined)
   * @param method  the invoked method name
   * @param desc    collection method description (type signature - without generic info)
   */
  public static void traceInvoked(Object colIns, String colCls, String method, String desc) {
    traceInvokedWithParam(colIns, colCls, method, desc, null);
  }

  /**
   * This method will be injected into Scala collection STATIC methods with no parameter list as:
   * object HashMap {
   *   def apply(): Unit = {
   *     CollectionTrace.traceInvoked("pkg/HashMap", "apply", "()V")
   *     ...
   *   }
   * }
   * @param colCls  collection class name (where the method is defined)
   * @param method  the invoked method name
   * @param desc    collection method description (type signature - without generic info)
   */
  public final static void traceStaticInvoked(String colCls, String method, String desc) {
    traceStaticInvokedWithParam(colCls, method, desc, null);
  }

  /**
   * This method will be injected into places before invoke the collection methods:
   * class Test {
   *  def test() {
   *    CollectionTrace.traceUserInvoked("pkg/Test", "test", "pkg/Range", "foreach", "(Function0)V")
   *    (1 to 10) foreach {...}
   *  }
   * }
   * @param invoker       the class name which invoke the collection method
   * @param callerMethod  the method name which invoke the collection method
   * @param colCls        the collection class name which is invoked
   * @param method        the collection method name which is invoked
   * @param desc          collection method description (type signature - without generic info)
   */
  public final static void traceUserInvoked(String invoker, String callerMethod, String colCls, String method, String desc) {
    String key = colCls + "::" + method + desc + " from " + invoker + "::" + callerMethod;
    traceInvokedImpl(callerStatistics, key);
  }

  private final static void traceInvokedImpl(ConcurrentHashMap<String, LongAdder> map, String key) {
    LongAdder adder = map.get(key);
    if (adder != null) {
      adder.increment();
    } else {
      LongAdder newAdder = new LongAdder();
      newAdder.increment();
      map.put(key, newAdder);
    }
  }

  public final static HashMap<String, Long> getStatisticAndReset() {
    return getStatisticAndResetImpl(statistics);
  }
  public final static HashMap<String, Long> getCallerStaticAndReset() {
    return getStatisticAndResetImpl(callerStatistics);
  }
  public final static HashMap<String, Long> getCtorStaticAndReset() { return getStatisticAndResetImpl(ctorStatistics); }

  private final static HashMap<String, Long> getStatisticAndResetImpl(ConcurrentHashMap<String, LongAdder> map) {
    HashMap<String, Long> result = new HashMap<String, Long>();
    map.forEach((k, adder) -> {
      result.put(k, adder.longValue());
      adder.reset();
    } );
    return result;
  }
}
