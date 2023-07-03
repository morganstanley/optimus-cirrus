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

import java.lang.instrument.*;
import java.security.ProtectionDomain;
import java.util.Collection;

import org.objectweb.asm.*;

/**
 * The necessary boilerplate to make MethodRewriteVisitor usable from an Instrumentation instance
 * (i.e. javaagent).
 *
 * <p>Don't forget to load the class for the toClass before instrumentation or you could get runtime
 * exceptions.
 */
class MethodRewriteTransformer implements ClassFileTransformer {

  private final String fromClass, fromMethod, toClass, toMethod;
  private final Collection<String> targets;

  // if targets is null, try to detect everywhere this could be
  // applicable (everything should be opt-in)
  MethodRewriteTransformer(
      Collection<String> targets,
      String fromClass,
      String fromMethod,
      String toClass,
      String toMethod) {
    this.targets = targets;
    this.fromClass = fromClass;
    this.fromMethod = fromMethod;
    this.toClass = toClass;
    this.toMethod = toMethod;
  }

  @Override
  public byte[] transform(
      ClassLoader loader,
      String className,
      Class<?> classBeingRedefined,
      ProtectionDomain protectionDomain,
      byte[] origBytes)
      throws IllegalClassFormatException {
    if (targets != null) {
      boolean match = false;
      for (String target : targets) {
        if (className.startsWith(target)) {
          match = true;
          break;
        }
      }
      if (!match) return null;
    }

    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
    MethodRewriteVisitor cv =
        new MethodRewriteVisitor(cw, fromClass, fromMethod, toClass, toMethod);
    ClassReader cr = new ClassReader(origBytes);
    cr.accept(cv, ClassReader.SKIP_FRAMES);

    if (!cv.isChanged()) {
      return null;
    }
    if (targets == null) {
      System.err.println("COULD MAKE CHANGES TO " + className);
      return null;
    }
    return cw.toByteArray();
  }
}
