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
package optimus;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import optimus.graph.DiagnosticSettings;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.util.ASMifier;
import org.objectweb.asm.util.Textifier;
import org.objectweb.asm.util.TraceClassVisitor;

public class BiopsyLab {
  private static final PrintStream stderr = System.err;

  public static class Biopsy {
    public final String className;
    public final byte[] original;
    public final byte[] transformed;

    Biopsy(String className, byte[] original, byte[] transformed) {
      super();
      this.className = className;
      this.original = original;
      this.transformed = transformed;
    }

    public String getHumanReadableTransformedBiopsy() {
      StringWriter writer = new StringWriter();
      new ClassReader(transformed).accept(new TraceClassVisitor(new PrintWriter(writer)), 0);
      return writer.toString();
    }
  }

  // Not expected to contain more than a dozen biopsies, so choosing low memory footprint over
  // faster access
  private static final List<Biopsy> biopsies = Collections.synchronizedList(new ArrayList<>());

  public static String byteCodeAsAsm(byte[] bytes) {
    StringWriter writer = new StringWriter();
    TraceClassVisitor traceClassVisitor =
        new TraceClassVisitor(null, new ASMifier(), new PrintWriter(writer));
    new ClassReader(bytes).accept(traceClassVisitor, 0);
    return writer.toString();
  }

  public static String byteCodeAsString(byte[] bytes) {
    StringWriter writer = new StringWriter();
    TraceClassVisitor traceClassVisitor =
        new TraceClassVisitor(null, new Textifier(), new PrintWriter(writer));
    new ClassReader(bytes).accept(traceClassVisitor, 0);
    return writer.toString();
  }

  static void dumpClass(String className, byte[] classfileBuffer) throws IOException {
    dumpClass(DiagnosticSettings.classDumpLocation, className, classfileBuffer);
  }

  static void dumpClass(String folder, String className, byte[] classfileBuffer)
      throws IOException {
    if (folder != null) {
      File parent = new File(folder);
      if (!parent.exists()) throw new VerifyError("could not read output directory " + folder);
      File classOutfile = new File(parent, className.replace('/', '.') + ".class");
      if (!classOutfile.createNewFile())
        throw new VerifyError("could not create output file " + classOutfile.getAbsolutePath());
      try (FileOutputStream fos = new FileOutputStream(classOutfile)) {
        fos.write(classfileBuffer);
      }
    } else {
      // let's not dump a raw classfile to stderr...
      new ClassReader(classfileBuffer).accept(new TraceClassVisitor(new PrintWriter(stderr)), 0);
    }
  }

  static void takeBiopsyOfClass(String className, byte[] original, byte[] transformed) {
    biopsies.add(new Biopsy(className, original, transformed));
  }

  private static String constructClassName(String className) {
    return className.replaceAll("\\.", "/");
  }

  public static Optional<Biopsy> getBiopsy(Class<?> clazz) {
    return biopsies.stream()
        .filter((biopsy -> biopsy.className.equals(constructClassName(clazz.getName()))))
        .findFirst();
  }
}
