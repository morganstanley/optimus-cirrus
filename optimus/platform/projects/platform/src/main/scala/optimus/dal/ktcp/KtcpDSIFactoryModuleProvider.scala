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
package optimus.dal.ktcp
import com.google.inject.AbstractModule
import com.google.inject.Provides
import com.google.inject.multibindings.MapBinder
import optimus.config.RuntimeConfiguration
import optimus.platform.dal.DSIURIScheme
import optimus.platform.dal.config.DalAsyncConfig
import optimus.platform.dsi.bitemporal.factory.DSIFactoryImpl
import optimus.platform.dsi.bitemporal.factory.DsiFactoryModule
import optimus.platform.dsi.bitemporal.factory.DsiFactoryModuleProvider

import java.util.Optional

class KtcpDSIFactoryModuleProvider extends DsiFactoryModuleProvider {
  override def makeModule(config: RuntimeConfiguration): DsiFactoryModule = new Module

  private class Module extends AbstractModule with DsiFactoryModule {
    override def configure(): Unit = {
      val dsiFactoryImplBinder = MapBinder
        .newMapBinder(binder, classOf[String], classOf[DSIFactoryImpl])
      dsiFactoryImplBinder.addBinding(DSIURIScheme.KTCP).to(classOf[KtcpDSIFactoryImpl])
      dsiFactoryImplBinder.addBinding(DSIURIScheme.SKTCP).to(classOf[SktcpDSIFactoryImpl])
    }

    @Provides
    def ktcpDSIFactoryImpl(config: RuntimeConfiguration, asyncConfigOpt: Optional[DalAsyncConfig]): KtcpDSIFactoryImpl =
      new KtcpDSIFactoryImpl(config, Option(asyncConfigOpt.orElse(null)))

    @Provides
    def sktcpDSIFactoryImpl(
        config: RuntimeConfiguration,
        asyncConfigOpt: Optional[DalAsyncConfig]): SktcpDSIFactoryImpl =
      new SktcpDSIFactoryImpl(config, Option(asyncConfigOpt.orElse(null)))
  }
}
