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
package optimus.graph.loom.compiler;

import optimus.graph.loom.TransformableMethod;
import org.objectweb.asm.tree.ClassNode;

/**
 * A central utility to raise loom related errors during the loom bytecode manipulations, mainly for
 * easier debugging!
 */
public class LMessage {

  public static void fatal(String msg) {
    throw new RuntimeException("LFATAL : " + msg);
  }

  public static void fatal(String className, Exception e) {
    throw new RuntimeException("LFATAL : (" + className + ") " + e.getMessage(), e);
  }

  static void error(String msg) {
    System.err.println("LERROR: " + msg);
  }

  public static void require(boolean cond, String msg) {
    if (!cond) error("require failure: " + msg);
  }

  public static void warning(String msg, TransformableMethod tmethod, ClassNode cls) {
    var methodName = cls.name + "." + tmethod.method.name;
    System.err.println(
        "LWARNING: " + msg + methodName + " (" + cls.sourceFile + ":" + tmethod.lineNumber + ")");
  }

  static void info(String msg, TransformableMethod tmethod, ClassNode cls) {
    var methodName = cls.name + "." + tmethod.method.name;
    System.err.println(
        "LINFO: " + msg + methodName + " (" + cls.sourceFile + ":" + tmethod.lineNumber + ")");
  }
}
