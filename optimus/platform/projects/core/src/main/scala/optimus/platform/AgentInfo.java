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
package optimus.platform;

import java.util.Arrays;
import java.util.List;

import com.ms.zookeeper.clientutils.ZkEnv;
import optimus.breadcrumbs.*;
import optimus.breadcrumbs.crumbs.*;

public class AgentInfo {
  private static String singleAgentInfo() {
    /* Entityagengt bytecode transformation inserts code like
       return "Entity Agent (v8):p " +
     which would have caused an "unreachable code" error had we compiled it from source, but
     causes no problems when inserted as bytecode.
    */
    String version = System.getProperty("ENTITY_AGENT_VERSION");
    if (version == null) {
      BreadcrumbsSetupFactory$.MODULE$.initialization(ZkEnv.qa, "dev", "agentInfo");
      Breadcrumbs.send(
          PropertiesCrumb.apply(
              ChainedID$.MODULE$.root(),
              Crumb.OptimusSource$.MODULE$,
              Properties.entityAgentVersion().apply("none"),
              Properties.user().apply(System.getProperty("user.name")),
              Properties.cmdLine().apply(System.getProperty("sun.java.command"))));
      Breadcrumbs.flush();
      throw new IllegalStateException(
          "EntityAgent version not found - somehow you are missing the "
              + "'-javaagent' argument for entityagent - Contact Optimus Graph Team");
    }
    return version;
  }

  // Someday, we may handle multiple agents.
  public static List<String> agentInfo() {
    return Arrays.asList(singleAgentInfo());
  }
}
