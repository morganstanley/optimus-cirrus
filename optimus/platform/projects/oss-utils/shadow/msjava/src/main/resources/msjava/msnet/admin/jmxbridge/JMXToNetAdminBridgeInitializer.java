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

package msjava.msnet.admin.jmxbridge;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.management.MBeanServer;
import msjava.base.admin.MSAdminCommand;
import msjava.base.admin.MSAdminCommandStore;
import msjava.management.jmx.remote.connector.AbstractJMXServer;
import msjava.msnet.admin.MSNetAdminManager;
import msjava.msnet.spring.AdminManagerAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.Lifecycle;
public class JMXToNetAdminBridgeInitializer implements InitializingBean, AdminManagerAware, Lifecycle {
    private AbstractJMXServer jmxServer;
    private MBeanServer mbeanServer;
    private MSNetAdminManager adminManager;
    private JMXToNetAdminNamingPolicy jmxToNetAdminNamingPolicy;
    
    private MBeanToNetAdminOutputFormatter outputFormatter;
    private JMXToNetAdminExportPolicy jmxToNetAdminExportPolicy;
    private final AtomicBoolean configured = new AtomicBoolean(false);
    
    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private void throwIfConfigured() {
        if (configured.get()) {
            throw new IllegalStateException("Already configured");
        }
    }
    public AbstractJMXServer getJmxServer() {
        return jmxServer;
    }
    public void setJmxServer(AbstractJMXServer jmxServer) {
        throwIfConfigured();
        this.jmxServer = jmxServer;
    }
    public MSNetAdminManager getAdminManager() {
        return adminManager;
    }
    @Override
    public void setAdminManager(MSNetAdminManager adminManager) {
        this.adminManager = adminManager;
    }
    public JMXToNetAdminExportPolicy getJmxToNetAdminExportPolicy() {
        return jmxToNetAdminExportPolicy;
    }
    public void setJmxToNetAdminExportPolicy(JMXToNetAdminExportPolicy jmxToNetAdminExportPolicy) {
        this.jmxToNetAdminExportPolicy = jmxToNetAdminExportPolicy;
    }
    
    public void setOutputFormatter(MBeanToNetAdminOutputFormatter outputFormatter) {
        this.outputFormatter = outputFormatter;
    }
    
    public JMXToNetAdminNamingPolicy getJmxToNetAdminNamingPolicy() {
        return jmxToNetAdminNamingPolicy;
    }
    public void setJmxToNetAdminNamingPolicy(JMXToNetAdminNamingPolicy jmxToNetAdminNamingPolicy) {
        this.jmxToNetAdminNamingPolicy = jmxToNetAdminNamingPolicy;
    }
    public MBeanServer getMBeanServer() {
        return mbeanServer;
    }
    public void setMBeanServer(MBeanServer mbeanServer) {
        this.mbeanServer = mbeanServer;
    }
    @Override
    public void afterPropertiesSet() throws Exception {
        if (configured.compareAndSet(false, true)) {
            if (adminManager == null) {
                throw new IllegalArgumentException("msnetAdminManager should be set");
            }
            if (jmxServer == null && mbeanServer == null) {
                throw new IllegalArgumentException("either jmxServer or mbeanServer should be set");
            }
            if (jmxServer != null && mbeanServer != null) {
                throw new IllegalArgumentException("jmxServer and mbeanServer cannot be set at the same time.");
            }
            if (jmxServer != null) {
                
                mbeanServer = jmxServer.getMbeanServerForwarder();
            }
        } else {
            throw new IllegalStateException("Already configured");
        }
    }
    
    @Override
    public boolean isRunning() {
        return isStarted.get();
    }
    
    @Override
    public void start() {
        if(isStarted.compareAndSet(false, true)){
            
            new JMXToNetAdminBridge(mbeanServer, adminManager, jmxToNetAdminNamingPolicy, jmxToNetAdminExportPolicy, this.outputFormatter);
        }
        
    }
    
    @Override
    public void stop() {
        
    }
}
