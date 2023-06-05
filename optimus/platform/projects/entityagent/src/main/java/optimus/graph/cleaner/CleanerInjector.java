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

import static optimus.debug.InstrumentationConfig.CLEANABLE_FIELD_NAME;
import static optimus.debug.InstrumentationConfig.CLEANABLE_TYPE;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;

import optimus.graph.DiagnosticSettings;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Opcodes;

public class CleanerInjector implements ClassFileTransformer, Opcodes {

  @Override
  public byte[] transform(ClassLoader loader, String className, Class<?> classBeingRedefined,
      ProtectionDomain protectionDomain, byte[] bytes) throws IllegalClassFormatException {
    // do nothing if not from an package to rewrite or configuration is missing
    var packageToRewrite = DiagnosticSettings.disposablePackageToRewrite;
    if (packageToRewrite == null || !className.startsWith(packageToRewrite)) return bytes;

    ClassReader crSource = new ClassReader(bytes);

    // do nothing if not implementing disposable
    if (!implementsDisposable(crSource)) return bytes;

    ClassWriter cw = new ClassWriter(crSource, ClassWriter.COMPUTE_FRAMES);
    generateCleanableField(cw);
    ClassVisitor cv = new CleanerInjectorAdapter(className, cw);
    crSource.accept(cv, ClassReader.SKIP_FRAMES);
    return cw.toByteArray();
  }

  private static boolean implementsDisposable(ClassReader crSource) {
    // do nothing if an interface was not given
    var interfaceToRewrite = DiagnosticSettings.disposableInterfaceToRewrite;
    if (interfaceToRewrite == null) return false;

    String[] interfaces = crSource.getInterfaces();
    for (String iface: interfaces)
      if (iface.equals(interfaceToRewrite))
        return true;
    return false;
  }

  private void generateCleanableField(ClassWriter cw) {
    FieldVisitor fv = cw.visitField(ACC_PRIVATE, CLEANABLE_FIELD_NAME, CLEANABLE_TYPE.getDescriptor(), null, null);
    fv.visitEnd();
  }

}