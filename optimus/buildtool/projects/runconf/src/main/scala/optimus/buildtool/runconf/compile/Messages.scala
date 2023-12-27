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
package optimus.buildtool.runconf.compile

import RunConfSupport.names
import optimus.buildtool.runconf.ScopedName

private[compile] object Messages {

  def invalidSubstitution(substitution: String): String = {
    s"Could not resolve substitution to a value: $substitution"
  }

  def propertyOnlyAllowedForTests(name: String): String = {
    s"Property '$name' can only be used for test configuration"
  }

  def propertyOnlyAllowedForApps(name: String): String = {
    s"Property '$name' can only be used for app configuration"
  }

  def typeMismatch(expected: Type, actual: Type): String = {
    s"Type mismatch. Expected $expected but found $actual"
  }

  val invalidBase = "Parent definition has errors"

  def invalidRunConfType(actual: Type): String = {
    s"Run configuration must be an object but found $actual"
  }

  def unknownProperty(name: String): String = {
    s"Unknown property '$name'"
  }

  val selfInheritance = "Run configuration inherits from itself"

  def noSuchRunConf(name: String): String = {
    s"Run configuration '$name' is not defined"
  }

  val appRunConfsRequiresMainClass = "Application run configuration requires 'mainClass' property"

  def invalidSetOfProperties(params: Set[String]): String = {
    val stringified = formatSet(params)
    s"Properties $stringified cannot be defined together"
  }

  def cyclicInheritance(configs: Set[ScopedName]): String = {
    val hint = if (configs.exists(scopedName => names.allDefaultTemplates.contains(scopedName.name))) {
      ". Note that implicit templates may only inherit from other templates."
    } else ""
    val stringified = formatSet(configs.map(_.toString))
    s"Cyclic inheritance between $stringified" + hint
  }

  private def formatSet(configs: Set[String]) = {
    val sortedAndQuoted = configs.toSeq.sorted.map(p => s"'$p'")
    if (sortedAndQuoted.size == 1) sortedAndQuoted.head
    else {
      sortedAndQuoted.dropRight(1).mkString(", ") + " and " + sortedAndQuoted.last
    }
  }

  val parentHasErrors = "Parent definition has errors"

  val testInApplicationsBlock = "Test configurations are not allowed in applications block"

  val appDirNotForTests = "module.appDir is not available for test configurations"

  val dtcNotForApps: String = "Distributed Test Cache is not supported for applications"

  val unusedTemplate = "This template is unused"

  def duplicatedRunconfName(appName: String) = s"Duplicated RunConf name: $appName"

  def atImplicitParent(parent: String, messageToWrap: String): String = {
    s"$messageToWrap (at implicit parent '$parent')"
  }

  def testInheritsFromApp(property: String): String = {
    s"Test cannot inherit from this configuration because it contains app only property '$property'"
  }
}
