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
package optimus.systemexit;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;

import optimus.MethodRewriteVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;

/**
 * Replaces java.lang.System.exit method calls with optimus.systemexit.SystemExitReplacement.exit
 * method to prevent test failures from the Test Worker for applications' tests that invoke
 * java.lang.System.exit.
 */
public class SystemExitTransformer implements ClassFileTransformer {
  @Override
  public byte[] transform(ClassLoader loader, String className, Class<?> classBeingRedefined,
      ProtectionDomain protectionDomain, byte[] classfileBuffer) throws IllegalClassFormatException {
    if (SystemExitExemption.isClassExempt(className)) {
      return null; // see doc comment on SystemExitExemption
    }
    ClassWriter out = new ClassWriter(0);
    MethodRewriteVisitor rewriter = new MethodRewriteVisitor(
        out,
        "java/lang/System", "exit",
        "optimus/systemexit/SystemExitReplacement", "exit"
    );
    new ClassReader(classfileBuffer).accept(rewriter, 0);
    if (rewriter.isChanged()) return out.toByteArray();
    else return null;
  }
}