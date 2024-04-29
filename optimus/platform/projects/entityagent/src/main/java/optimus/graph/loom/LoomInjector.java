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

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassReaderEx;
import org.objectweb.asm.ClassWriter;
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
    crSource.accept(cls, ClassReader.SKIP_FRAMES);
    var adapter = new LoomAdapter(cls);
    adapter.transform();
    ClassWriter cw = new ClassWriter(crSource, ClassWriter.COMPUTE_FRAMES);
    cls.accept(cw);
    return cw.toByteArray();
  }

  private ClassReader shouldBeTransformed(ProtectionDomain domain, byte[] bytes) {
    // The quickest check to ignore class we should not possibly be transforming
    if (domain == null) return null;
    // Note: consider quick exclusion for example based on package name
    // See notes on @loom annotation
    ClassReaderEx crSource = new ClassReaderEx(bytes);
    return crSource.hasAnnotation(LoomConfig.LoomAnnotation) ? crSource : null;
  }
}
