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
package optimus.platform.breadcrumbs

import com.ms.zookeeper.clientutils.ZkEnv
import optimus.breadcrumbs.Breadcrumbs
import optimus.config.RuntimeConfiguration
import optimus.graph.diagnostics.sampling.SamplingProfilerSwitch
import optimus.platform.UntweakedScenarioState
import optimus.platform.BreadcrumbsSetupFactory
import optimus.platform.EvaluationContext
import optimus.platform.RuntimeEnvironment
import optimus.platform.breadcrumbs.BreadcrumbsSetup.Flags
import optimus.platform.dal.config.DalAppId
import optimus.platform.runtime.MockRuntimeConfiguration
import optimus.platform.runtime.RuntimeComponents
import optimus.platform.runtime.ZkUtils

class BreadcrumbsSetup extends BreadcrumbsSetupFactory {
  def initialization(zkEnv: ZkEnv, name: String, app: String): Unit =
    BreadcrumbsSetup.initializeBreadcrumbsEnv(zkEnv, name, app, Flags.DoNotInitializeEnv)
}

object BreadcrumbsSetup {
  sealed trait Flag

  type Flags = Set[Flag]
  object Flags {
    case object DoNotInitializeEnv extends Flag
    case object SingleThreadedRuntime extends Flag

    val None = Set.empty[Flag]
  }

  // This is the main breadcrumbs initialization API method for components
  // that are in the codetree but do not extend OptimusApp/OptimusTask.
  // Note: Preliminary breadcrumbs infrastructure configuration is required,
  // which will determine the correct values for the "name" and "app" parameters.
  // Please reach out to breadcrumbs-interest before attempting to use this API.

  // After invoking this method, the calling thread will have a working breadcrumbs
  // environment and can transparently use APIs such as Breadcrumbs.info().
  // If multiple threads are used or required, use BreadcrumbsSetup.createBreadcrumbsEnabledUntweakedScenarioState
  // to retrieve a prepared untweaked scenario state, then invoke BreadcrumbsSetup.ensureUntweakedScenarioStateIsInitialized
  // with the untweaked scenario state returned by the first API to prepare each thread for breadcrumb publishing.
  def initializeBreadcrumbsEnv(zkEnv: ZkEnv, name: String, app: String): Unit =
    createBreadcrumbsEnabledUntweakedScenarioState(zkEnv, name, app, Flags.None)

  def initializeBreadcrumbsEnv(zkEnv: ZkEnv, name: String, app: String, flags: Flag*): Unit =
    createBreadcrumbsEnabledUntweakedScenarioState(zkEnv, name, app, flags.toSet)

  // Advanced: Use this method to obtain a untweaked scenario state prepared for using breadcrumbs.
  // If the DoNotInitializeEnv flag is passed, the environment will not be initialized immediately,
  // which can be useful in multithreaded applications where the calling thread will not
  // send any breadcrumbs, but other threads may; in this case, the returned untweaked scenario state
  // needs to be passed to BreadcrumbsSetup.ensureUntweakedScenarioStateIsInitialized in each candidate thread.
  def createBreadcrumbsEnabledUntweakedScenarioState(
      zkEnv: ZkEnv,
      name: String,
      app: String,
      flags: Flag*): UntweakedScenarioState = {
    createBreadcrumbsEnabledUntweakedScenarioState(zkEnv, name, app, flags.toSet)
  }

  private def createBreadcrumbsEnabledUntweakedScenarioState(
      zkEnv: ZkEnv,
      name: String,
      app: String,
      flags: Flags): UntweakedScenarioState = {
    val currentConfig: RuntimeConfiguration = new MockRuntimeConfiguration(name, DalAppId(app))
    val rootContext = ZkUtils.getRootContextAndEnv(zkEnv, false)._1
    Breadcrumbs.customizedInit(currentConfig.propertyMap, rootContext)
    SamplingProfilerSwitch.configure(currentConfig, rootContext)
    val untweakedScenarioState = UntweakedScenarioState(
      new RuntimeEnvironment(new RuntimeComponents(currentConfig), null))
    if (!flags.contains(Flags.DoNotInitializeEnv))
      ensureGraphIsInitialized(untweakedScenarioState, flags.contains(Flags.SingleThreadedRuntime))
    untweakedScenarioState
  }

  // Advanced: Use this method in a multithreaded setup to initialize the untweaked scenario state
  // obtained from BreadcrumbsSetup.createBreadcrumbsEnabledUntweakedScenarioState in each thread
  // you intend to publish breadcrumbs from.
  // This method is inexpensive and idempotent.
  def ensureGraphIsInitialized(
      untweakedScenarioState: UntweakedScenarioState,
      singleThreaded: Boolean = false): Unit = {
    if (singleThreaded) EvaluationContext.initializeWithNewInitialTimeSingleThreaded(untweakedScenarioState)
    else EvaluationContext.initializeWithNewInitialTime(untweakedScenarioState)
  }

  def main(args: Array[String]): Unit = {
    import optimus.breadcrumbs._
    import optimus.breadcrumbs.crumbs._

    println("Breadcrumbs initialization demo")
    BreadcrumbsSetup.initializeBreadcrumbsEnv(ZkEnv.dev, "demo", "BreadcrumbsInitializationDemo")
    Breadcrumbs.info(
      ChainedID.create(),
      PropertiesCrumb(_, new Crumb.Source { override val name = "DEMO" }, Map("hello" -> "dev world")))
    Breadcrumbs.flush()
  }
}
