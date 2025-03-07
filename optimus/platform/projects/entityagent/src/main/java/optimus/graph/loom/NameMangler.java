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
package optimus.graph.loom;

import static optimus.CoreUtils.stripPrefix;
import static optimus.CoreUtils.stripSuffix;

/**
 * A central place for consistently generate $newNode, $queued, $_ and other names, as well as
 * dealing with Scala's mangling of names (which happens for interfaces, private package classes,
 * private constructors and more!)
 */
public class NameMangler {

  private static final String QUEUED_SUFFIX = "$queued";
  private static final String NEW_NODE_SUFFIX = "$newNode";
  private static final String LOOM_SUFFIX = "$_";
  private static final String PLAIN_SUFFIX = "__";
  private static final String IMPL_SUFFIX = "$impl";

  static boolean isQueued(String methodName) {
    return methodName.endsWith(QUEUED_SUFFIX);
  }

  static boolean isNewNode(String methodName) {
    return methodName.endsWith(NEW_NODE_SUFFIX);
  }

  static boolean isImpl(String methodName) {
    return methodName.endsWith(IMPL_SUFFIX);
  }

  public static String mangleName(Class<?> cls) {
    return mangleName(cls.getName(), '.');
  }

  public static String mangleName(String jvmClsName) {
    return mangleName(jvmClsName, '/');
  }

  private static String mangleName(String name, char c) {
    return name.replace(c, '$') + '$';
  }

  public static String mkNewNodeName(String mangledClsName, String methodName) {
    return unmangleName(mangledClsName, methodName) + NEW_NODE_SUFFIX;
  }

  public static String mkQueuedName(String mangledClsName, String methodName) {
    return unmangleName(mangledClsName, methodName) + QUEUED_SUFFIX;
  }

  public static String mkLoomName(String mangledClsName, String methodName) {
    return unmangleName(mangledClsName, methodName) + LOOM_SUFFIX;
  }

  public static String mkImplName(String mangledClsName, String methodName) {
    return unmangleName(mangledClsName, methodName) + IMPL_SUFFIX;
  }

  public static String mkPlainName(String mangledClsName, String methodName) {
    return unmangleName(mangledClsName, methodName) + PLAIN_SUFFIX;
  }

  static String unmangleName(Class<?> cls, String methodName) {
    var mangledClsName = mangleName(cls);
    return unmangleName(mangledClsName, methodName);
  }

  static String unmangleName(String mangledClsName, String methodName) {
    if (methodName.startsWith(mangledClsName)) {
      var cleanName = methodName.substring(mangledClsName.length());
      // Scala can mangle names with either single or double '$'!
      return stripPrefix(cleanName, "$");
    }
    return methodName;
  }

  public static String stripNewNodeSuffix(String methodName) {
    return stripSuffix(methodName, NEW_NODE_SUFFIX);
  }

  public static String stripQueuedSuffix(String methodName) {
    return stripSuffix(methodName, QUEUED_SUFFIX);
  }

  public static String stripImplSuffix(String methodName) {
    return stripSuffix(methodName, IMPL_SUFFIX);
  }
}
