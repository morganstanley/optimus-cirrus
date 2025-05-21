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

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import msjava.slf4jutils.scalalog.Logger;
import org.objectweb.asm.*;

public class SourceLocator {

  public static final Logger log =
      msjava.slf4jutils.scalalog.package$.MODULE$.getLogger("optimus.graph");

  private static final ConcurrentHashMap<Class<?>, StackTraceElement> SOURCE_OF_CACHE =
      new ConcurrentHashMap<>();

  public static String sourceOf(Class<?> cls) {
    StackTraceElement te = traceElement(cls);
    return te.getFileName() + ":" + te.getLineNumber();
  }

  public static StackTraceElement traceElement(Class<?> cls) {
    return SOURCE_OF_CACHE.computeIfAbsent(cls, SourceLocator::calcSourceOf);
  }

  // we want to get the starting point of the user-written code which will be in
  // func/funcFSM/childNode for compiler generated nodes or run for manual nodes
  private static final String[] PREFERRED_METHODS = {"funcFSM", "func", "childNode", "run"};

  private static String getBestMethod(Method[] methods) {
    for (String preferred : PREFERRED_METHODS) {
      for (Method actual : methods) {
        if (actual.getName().equals(preferred)) return preferred;
      }
    }
    // we didn't find a preferred method, so we'll scan all methods instead
    return null;
  }

  private static StackTraceElement calcSourceOf(Class<?> cls) {
    // n.b. "declared" methods are those actually implemented on this class, excluding inherited
    // implementations (i.e. declared methods will have line numbers in this class, others won't)
    WatchForLines w4l = new WatchForLines(getBestMethod(cls.getDeclaredMethods()));
    try {
      ClassLoader cl = cls.getClassLoader();
      if (cl == null) return new StackTraceElement("", "", "[generated]", -1);
      ClassReader cr = fromResource(cl, cls.getName().replace('.', '/') + ".class");
      cr.accept(w4l, 0);
    } catch (Exception e) {
      log.javaLogger().info("Ignoring exception thrown in SourceLocator.calcSourceOf", e);
    }
    return new StackTraceElement(cls.getName(), "", w4l.source, w4l.lineNumber);
  }

  @SuppressWarnings("unused")
  static byte[] bytesFromResource(Class<?> cls) throws IOException {
    return bytesFromResource(cls.getClassLoader(), cls.getName().replace('.', '/') + ".class");
  }

  private static byte[] bytesFromResource(ClassLoader cl, String name) throws IOException {
    InputStream is = cl.getResourceAsStream(name);
    if (is == null) {
      throw new IOException("Cannot find class " + name);
    }
    int INITIAL_ARRAY_BYTES = 8192;
    byte[] bytes = new byte[INITIAL_ARRAY_BYTES];
    int size = 0;
    while (true) {
      int readSize = is.read(bytes, size, bytes.length - size);
      if (readSize == -1) break;
      size += readSize;
      if (size == bytes.length) bytes = Arrays.copyOf(bytes, bytes.length * 2);
    }
    is.close();
    return Arrays.copyOf(bytes, size);
  }

  private static ClassReader fromResource(ClassLoader cl, String name) throws IOException {
    return new ClassReader(bytesFromResource(cl, name));
  }
}

class WatchForLines extends ClassVisitor implements Opcodes {
  private final String preferredMethod;
  public String source = "not found";
  public int lineNumber = Integer.MAX_VALUE;

  WatchForLines(String preferredMethod) {
    super(ASM9, null);
    this.preferredMethod = preferredMethod;
  }

  private final MethodVisitor mv =
      new MethodVisitor(ASM9) {
        public void visitLineNumber(int arg0, Label arg1) {
          lineNumber = Math.min(arg0, lineNumber);
        }
      };

  public MethodVisitor visitMethod(
      int access, String name, String desc, String signature, String[] exceptions) {
    if (preferredMethod == null || preferredMethod.equals(name)) return mv;
    else return null;
  }

  public void visitSource(String source, String arg1) {
    this.source = source;
  }
}
