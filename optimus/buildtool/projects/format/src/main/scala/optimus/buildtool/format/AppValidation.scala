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
package optimus.buildtool.format

import com.typesafe.config.Config
import optimus.buildtool.runconf.AppRunConf
import optimus.buildtool.runconf.Validator

import scala.collection.compat._
import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq

final case class AppValidator(rules: Seq[Rule]) extends Validator[AppRunConf] {
  def validationErrors(app: AppRunConf): Seq[String] =
    rules.filter(validator => !validator.isValid(app)).map(_.message)
}

object AppValidator {
  private object names {
    val appValidation = "appValidation"
    val when = "when"
    val and = "and"
    val or = "or"
    val not = "not"
    val message = "message"
    val parameter = "parameter"
    val key = "key"
    val contains = "contains"
    val valueContains = "valueContains"

    val env = "env"
    val javaOpts = "javaOpts"
  }

  val Empty = AppValidator(Seq.empty)

  def load(loader: ObtFile.Loader): Result[AppValidator] =
    loader(ApplicationValidation).map(load)

  def load(conf: Config): AppValidator =
    if (conf.hasPath(names.appValidation)) {
      val ruleConfig = conf.getConfigList(names.appValidation).asScala.to(Seq)
      val rules = ruleConfig.map(genAppValidation)
      AppValidator(rules)
    } else AppValidator.Empty

  private def genAppValidation(conf: Config): Rule = {
    def parseCondition(conf: Config): Condition = {
      if (conf.hasPath(names.when)) {
        val when = conf.getConfig(names.when)
        when.getString(names.parameter) match {
          case names.env =>
            val valueContains = if (when.hasPath(names.valueContains)) when.getString(names.valueContains) else ""
            WhenEnv(when.getString(names.key), valueContains)
          case names.javaOpts => WhenJavaOpts(when.getString(names.contains))
        }
      } else if (conf.hasPath(names.not)) {
        Not(parseCondition(conf.getConfig(names.not)))
      } else if (conf.hasPath(names.and)) {
        And(conf.getConfigList(names.and).asScala.to(Seq).map(parseCondition))
      } else if (conf.hasPath(names.or)) {
        Or(conf.getConfigList(names.or).asScala.to(Seq).map(parseCondition))
      } else {
        throw new RuntimeException("Should not happen")
      }
    }

    val msg = conf.getString(names.message)
    Rule(msg, parseCondition(conf))
  }
}

final case class Rule(message: String, condition: Condition) {
  def isValid(value: AppRunConf): Boolean = !condition.check(value)
}

sealed trait Condition {
  def check(value: AppRunConf): Boolean
}

final case class WhenJavaOpts(contains: String) extends Condition {
  def check(value: AppRunConf): Boolean = value.javaOpts.exists(_.contains(contains))
}

final case class WhenEnv(key: String, valueContains: String) extends Condition {
  def check(value: AppRunConf): Boolean = value.env.get(key) match {
    case None                                => false
    case Some(env) if valueContains.nonEmpty => env.contains(valueContains)
    case Some(_)                             => true
  }
}

final case class Not(condition: Condition) extends Condition {
  def check(value: AppRunConf): Boolean = !condition.check(value)
}

final case class And(conditions: Seq[Condition]) extends Condition {
  def check(value: AppRunConf): Boolean = conditions.forall(_.check(value))
}

final case class Or(conditions: Seq[Condition]) extends Condition {
  def check(value: AppRunConf): Boolean = conditions.exists(_.check(value))
}
