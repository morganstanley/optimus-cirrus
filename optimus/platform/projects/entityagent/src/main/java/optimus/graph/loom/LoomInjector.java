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

import static org.objectweb.asm.Opcodes.ASM9;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.security.ProtectionDomain;
import optimus.graph.loom.compiler.LMessage;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassReaderEx;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.tree.ClassNode;

/** In doubt see: var byteCode = BiopsyLab.byteCodeAsString(cw.toByteArray()); */
public class LoomInjector implements ClassFileTransformer {

  @Override
  public byte[] transform(
      ClassLoader loader,
      String className,
      Class<?> classBeingRedefined,
      ProtectionDomain protectionDomain,
      byte[] bytes)
      throws IllegalClassFormatException {

    var crSource = shouldBeTransformed(protectionDomain, bytes);
    if (crSource == null) return bytes;

    var cls = new ClassNode();
    crSource.accept(cls, ClassReader.EXPAND_FRAMES);
    var adapter = new LoomAdapter(cls);
    adapter.transform();
    var cwe = new ClassWriterEx(crSource, adapter);
    var cw = new ClassVisitorEx(cwe, adapter);
    try {
      cls.accept(cw);
    } catch (Exception e) {
      LMessage.fatal(className, e);
      throw e;
    }

    // Used for testing only!
    if (adapter.debugRetainModifiedByteCode()) ClassNodeReporter.report(cls);

    // [SEE_LOOM_CM_PLAYGROUND]
    // consider uncommenting these lines for easier debugging
    //    if (className.endsWith("ConcurrencyTestWIP$")) {
    //      System.err.println("Writing out: " + className);
    //      BiopsyLab.dumpClass(".", className, /*bytes*/ cwe.toByteArray());
    //    }

    return cwe.toByteArray();
  }

  static class ClassVisitorEx extends ClassVisitor {
    private static final VarHandle computeVH;

    static {
      try {
        var lookup = MethodHandles.privateLookupIn(ClassWriter.class, MethodHandles.lookup());
        computeVH = lookup.findVarHandle(ClassWriter.class, "compute", int.class);
      } catch (IllegalAccessException | NoSuchFieldException e) {
        throw new RuntimeException(e);
      }
    }

    private final ClassWriterEx cwe;
    private final LoomAdapter adapter;

    public ClassVisitorEx(ClassWriterEx cwe, LoomAdapter adapter) {
      super(ASM9, cwe);
      this.cwe = cwe;
      this.adapter = adapter;
    }

    @Override
    public MethodVisitor visitMethod(
        int access, String name, String descriptor, String signature, String[] exceptions) {
      int COMPUTE_ALL_FRAMES = 4; // From MethodWriter
      var prevValue = computeVH.get(cwe);
      if (adapter.needsToComputeFrames(name, descriptor)) {
        computeVH.set(cwe, COMPUTE_ALL_FRAMES);
      }
      try {
        return super.visitMethod(access, name, descriptor, signature, exceptions);
      } finally {
        computeVH.set(cwe, prevValue);
      }
    }
  }

  static class ClassWriterEx extends ClassWriter {
    private final LoomAdapter adapter;

    public ClassWriterEx(ClassReader classReader, LoomAdapter adapter) {
      super(classReader, COMPUTE_MAXS);
      this.adapter = adapter;
    }

    @Override
    protected String getCommonSuperClass(String type1, String type2) {
      return adapter.getCommonType(type1, type2);
    }
  }

  private static ClassReader shouldBeTransformed(ProtectionDomain domain, byte[] bytes) {
    // The quickest check to ignore class we should not possibly be transforming
    if (domain == null) return null;
    // Note: consider quick exclusion for example based on package name
    // See notes on @loom annotation
    ClassReaderEx crSource = new ClassReaderEx(bytes);
    return crSource.hasAnnotation(LoomConfig.LoomAnnotation) ? crSource : null;
  }
}
