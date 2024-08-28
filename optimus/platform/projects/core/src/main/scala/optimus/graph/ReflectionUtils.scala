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
package optimus.graph

import java.lang.reflect.Method

private[optimus] trait ReflectionUtils {

  // POSSIBLE IMPROVEMENT - can we somehow detect private fields so that we do not have to iterate the array
  // and just do a direct lookup by name?
  protected def lookupMethod[T](clss: Class[T], methodName: String): Method = {
    // WHY EXPANDED NAME? Scalac emits private methods that are referenced from inner classes or companions
    // as public in bytecode. In this case, the name is expanded to include the enclosing class as a prefix
    val expandedMethodName = s"${clss.getName.replaceAll("\\.", "\\$")}$$$$$methodName"

    val validMethodNames = List(methodName, expandedMethodName)
    val candidateMethods =
      clss.getDeclaredMethods.filter(method => validMethodNames.contains(method.getName) && !method.isSynthetic)
    candidateMethods match {
      case Array()       => explodeWithNoCandidateFound(clss, methodName)
      case Array(method) => reflect.ensureAccessible(method)
      case methods       => explodeWithNoUniqueCandidateFound(clss, methodName, methods)
    }
  }

  private def explodeWithNoCandidateFound[T](clss: Class[T], methodName: String) =
    throw new NoSuchMethodException(s""""Could not find a $methodName method for ${clss.getCanonicalName}.
           Declared Methods are: ${clss.getDeclaredMethods.mkString(", ")}.
           Methods are: ${clss.getMethods.mkString(", ")}.""")

  private def explodeWithNoUniqueCandidateFound[T](clss: Class[T], methodName: String, candidates: Array[Method]) =
    throw new NoSuchMethodException(s""""Could not uniquely identify a $methodName method for ${clss.getCanonicalName}.
           Selected candidates are: ${candidates.mkString(", ")}.
           Declared Methods are: ${clss.getDeclaredMethods.mkString(", ")}.
           Methods are: ${clss.getMethods.mkString(", ")}.""")
}
