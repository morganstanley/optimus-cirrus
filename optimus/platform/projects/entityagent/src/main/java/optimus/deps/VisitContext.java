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
package optimus.deps;

import static optimus.ClassMonitorInjector.classExtension;
import static optimus.ClassMonitorInjector.constructDependencyNameFromSimpleType;
import static optimus.EntityAgent.logErrMsg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import optimus.ClassMonitorInjector;

import org.objectweb.asm.Type;
import org.objectweb.asm.signature.SignatureReader;

public class VisitContext {
  public final ClassLoader loader;
  public final String className;
  final String classResourceName;

  // We should collect in a Set, then pass-on CMI when done.
  private final OptimusDependencyDiscoverySignatureVisitor signatureVisitor =
      new OptimusDependencyDiscoverySignatureVisitor(this);
  private final Set<String> collectedDependencies = new HashSet<>();
  private final List<VisitContext> childrenContext = new ArrayList<>();

  public VisitContext(ClassLoader loader, String className, String classResourceName) {
    super();

    this.loader = loader;
    this.className = className;
    this.classResourceName = classResourceName;
  }

  Set<String> getCollectedDependencies() {
    return new HashSet<>(collectedDependencies);
  }

  List<VisitContext> getChildrenContext() {
    return new ArrayList<>(childrenContext);
  }

  // WARNING: Dangerous with potential class shadowing/hacks
  //          But deemed reasonable for performance reason (reduce CPU load and minimize object
  // trashing).
  protected static boolean dependencyOfInterest(String name) {
    assert (name.contains("/") || (name.indexOf('.') == name.lastIndexOf('.')));
    return !name.startsWith("java/lang/")
        && !name.startsWith("java/util/")
        && !name.startsWith("java/io/Serializable")
        && !name.startsWith("scala/");
  }

  String addClassDependencyFromSimpleTypeDesc(String descriptor) {
    String resourceName = constructDependencyNameFromSimpleType(Type.getType(descriptor));
    addClassDependencyAsResource(resourceName);
    return resourceName;
  }

  void addClassDependencyFromMethodDesc(String descriptor) {
    addClassDependency(Type.getType(descriptor));
  }

  void addClassDependency(Type type) {
    if (type.getSort() == Type.METHOD) {
      for (Type argType : type.getArgumentTypes()) {
        addClassDependency(argType);
      }
      addClassDependency(type.getReturnType());
    } else {
      addClassDependencyAsResource(constructDependencyNameFromSimpleType(type));
    }
  }

  void addClassDependencyFromValue(Object value) {
    if (value instanceof Type) {
      addClassDependencyAsResource(constructDependencyNameFromSimpleType((Type) value));
    } else if (value != null
        && !value.getClass().isPrimitive()
        && !value.getClass().isSynthetic()
        && !value.getClass().getName().startsWith("java.lang.")) {
      logErrMsg("Unsupported '" + value + "'");
    }
  }

  void addClassDependenciesFromSignature(String signature) {
    if (signature != null) {
      new SignatureReader(signature).accept(signatureVisitor);
    }
  }

  void addClassDependencies(String[] classDependencies) {
    if (classDependencies != null && classDependencies.length > 0) {
      Arrays.asList(classDependencies).forEach(this::addClassDependency);
    }
  }

  private String toResourceName(String classNamish) {
    return classNamish.replace("[]", "").replace('.', '/') + classExtension;
  }

  void addClassDependency(String classDependencyName) {
    if (classDependencyName != null) {
      assert (!classDependencyName.endsWith(classExtension));
      String classResourceName = toResourceName(classDependencyName);
      if (dependencyOfInterest(classResourceName)) {
        addClassDependencyAsResourceInternal(classResourceName);
      }
    }
  }

  private void addClassDependencyAsResource(String classDependencyResourceName) {
    assert (classDependencyResourceName == null
        || classDependencyResourceName.endsWith(classExtension));
    if (classDependencyResourceName != null && dependencyOfInterest(classDependencyResourceName)) {
      addClassDependencyAsResourceInternal(classDependencyResourceName);
    }
  }

  private void addClassDependencyAsResourceInternal(String classDependencyResourceName) {
    // Do not call dependencyOfInterest() twice in flow
    assert (classDependencyResourceName != null);
    assert (classDependencyResourceName.endsWith(classExtension));
    if (!classResourceName.equals(classDependencyResourceName)) {
      // Exclude self
      collectedDependencies.add(classDependencyResourceName);
    }
  }

  <T, U extends T> T visitIfInterested(
      String classDependencyResourceName,
      T defaultVisitor,
      BiFunction<VisitContext, T, U> visitorFactory) {
    if (dependencyOfInterest(classDependencyResourceName)
        && ClassMonitorInjector.isOptimusClass(loader, classDependencyResourceName)) {
      return visitorFactory.apply(moveTo(classDependencyResourceName), defaultVisitor);
    }
    return defaultVisitor;
  }

  public void wrapUp() {
    childrenContext.forEach(VisitContext::wrapUp);
    if (ClassMonitorInjector.isOptimusClass(loader, classResourceName)) {
      collectedDependencies.forEach(
          dep -> ClassMonitorInjector.computeAndAddClassDependency(loader, classResourceName, dep));
    }
  }

  void visitCollectedDependencies(BiConsumer<String, Set<String>> visitor) {
    childrenContext.forEach(child -> child.visitCollectedDependencies(visitor));
    if (!collectedDependencies.isEmpty()) {
      visitor.accept(classResourceName, collectedDependencies);
    }
  }

  public VisitContext moveTo(String classResourceName) {
    VisitContext child =
        new VisitContext(loader, classResourceName.replace(classExtension, ""), classResourceName);
    childrenContext.add(child);
    return child;
  }
}
