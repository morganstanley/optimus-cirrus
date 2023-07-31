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
package optimus.breadcrumbs.crumbs

private[breadcrumbs] trait ValidationAction {
  import ValidationAction.log

  def warn(keyset: Set[String], props: Map[String, String]): Unit = {
    val keys = keyset toSeq
    val values = keys map { k =>
      props(k)
    }
    log.warn(
      s"Setting metadata key(s) ${keys.toString} with new value(s) ${values.toString}. \nBreadcrumbs have metadata keys: ${MetaKeys.keyset.toSeq.toString}. Overwriting those keys in your crumbs' properties can make your breadcrumbs difficult to trace!")
  }
}

private[breadcrumbs] object ValidationAction extends ValidationAction {
  import org.slf4j.Logger
  import org.slf4j.LoggerFactory

  val log: Logger = LoggerFactory.getLogger(classOf[ValidationAction])
  val Default: ValidationAction.type = this
}

abstract class CrumbValidator {
  def action: ValidationAction
  def validate(c: Crumb): Unit
}

private[breadcrumbs] class StandardCrumbValidator extends CrumbValidator {
  def action: ValidationAction = ValidationAction.Default
  def validate(c: Crumb): Unit = {
    val colliders = MetaKeys.keyset intersect c.stringProperties.keys.toSet
    if (colliders.nonEmpty) action.warn(colliders, c.stringProperties)
  }
}

private[breadcrumbs] object MetaKeys extends Enumeration {
  type MetaKeys = Value
  val src, uuid, t, tUTC, vuid = Value

  lazy val keyset: Set[String] = this.values map { k =>
    k.toString
  }
}
