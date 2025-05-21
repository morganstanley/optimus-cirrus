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
package msjava.management;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import msjava.management.annotation.MonitorCollection;
import msjava.management.internal.event.AgentRegistrationEvent;
import msjava.management.internal.event.MonitorAddedEvent;
import msjava.management.internal.event.MonitoringEvent;
import msjava.management.internal.event.MonitoringListener;
import msjava.management.internal.event.WindowedMonitorPollingIntervalChanged;
import msjava.management.jmx.MBeanExporter;
import msjava.management.monitor.ThroughputMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.jmx.export.naming.SelfNaming;
import org.springframework.scheduling.TaskScheduler;
@ManagedResource(description = "Central controller for the management framework.")
public class MonitoringManager implements MonitoringListener, SelfNaming, Clock {
    private static final Logger logger = LoggerFactory.getLogger(MonitoringManager.class);
    
    
    private static final MonitoringManager INSTANCE = new MonitoringManager(false);
    
    
    
    public static MonitoringManager getInstance() {
        return INSTANCE;
    }
    
    private MonitoringManager() {
        this(true);
    }
    
    private MonitoringManager(boolean throwException) {
        if (throwException) {
            throw new AssertionError(
                    "This constructor can not be called. Have you specified the factory-method=\"getInstance\" attribute in your MonitoringManager spring bean?");
        }
    }
    
    private MonitorCategory monitorLevel = MonitorCategory.PROD;
    
    private final Map<ObjectName, MonitorAgent> objectNameToAgentMap = new ConcurrentHashMap<ObjectName, MonitorAgent>();
    
    @SuppressWarnings("unused")
    @MonitorCollection(monitorID = "agentList", displayName = "Number of agents registered")
    private final Collection<MonitorAgent> agentListView = Collections.unmodifiableCollection(objectNameToAgentMap
            .values());
    
    private Map<ObjectName, List<MonitorDescriptor>> configuration;
    
    private Map<ObjectName, List<MonitorDescriptor>> unusedConfiguration;
    
    
    private List<MonitorConfigurator> monitorConfigurators = new LinkedList<MonitorConfigurator>();
    
    private final Map<Long, WindowedMonitorPollingTask> windowMonitorMap = new HashMap<Long, WindowedMonitorPollingTask>();
    
    
    private TaskScheduler taskScheduler;
    
    public void setTaskScheduler(TaskScheduler taskScheduler) {
        this.taskScheduler = taskScheduler;
    }
    
    private Clock clock = new MilliSecondClock();
    private long defaultSampledWindowPeriod;
    public long getDefaultSampledWindowPeriod() {
        return defaultSampledWindowPeriod;
    }
    public void setDefaultSampledWindowPeriod(long defaultSampledWindowPeriod) {
        if (defaultSampledWindowPeriod < 1) {
            throw new IllegalArgumentException("The default sample window period should be larger than 1ms.");
        }
        this.defaultSampledWindowPeriod = defaultSampledWindowPeriod;
    }
    
    void reset() {
        configuration = null;
        monitorConfigurators.clear();
        unusedConfiguration = null;
        objectNameToAgentMap.clear();
        defaultSampledWindowPeriod = 0;
        windowMonitorMap.clear();
    }
    
    public synchronized void registerAgent(ObjectName name, MonitorAgent agent) {
        objectNameToAgentMap.put(name, agent);
        applyConfigurationOverrides(name, agent);
        applyMonitorConfigurators(name, agent);
        agent.setMonitorCategory(MonitorCategory.valueOf(getMonitorLevel()));
        registerWindowedMonitors(agent);
    }
    private void registerWindowedMonitors(MonitorAgent agent) {
        for (Monitor m : agent) {
            if (m instanceof WindowedMonitor) {
                registerWindowedMonitor((WindowedMonitor) m);
            }
        }
    }
    
    private boolean isBuiltInWindowedMonitor(WindowedMonitor windowedMonitor) {
        return windowedMonitor instanceof ThroughputMonitor;
    }
    
    
    private void registerWindowedMonitor(WindowedMonitor windowedMonitor) {
        if(isBuiltInWindowedMonitor(windowedMonitor)) {
            return;
        }
        
        if(taskScheduler != null) {
            synchronized (windowMonitorMap) {
                Long windowPeriod = windowedMonitor.getWindowPeriod();
                WindowedMonitorPollingTask pollingTask = windowMonitorMap.get(windowPeriod);
                if (pollingTask == null) {
                    pollingTask = new WindowedMonitorPollingTask();
                    windowMonitorMap.put(windowPeriod, pollingTask);
                    taskScheduler.scheduleWithFixedDelay(pollingTask, delay(windowPeriod), windowPeriod);
                }
                pollingTask.addWindowedMonitor(windowedMonitor);
            }
        } else {
            logger.error("WindowedMonitorScheduler property is not set on the MonitoringManager. In order to make {} work, please set WindowedMonitorScheduler property.", windowedMonitor.getClass().getName());
        }
    }
    private Date delay(long delay) {
        return new Date(System.currentTimeMillis() + delay);
    }
    
    
    public synchronized boolean unregisterAgent(ObjectName name, MonitorAgent agent) {
        return agent == objectNameToAgentMap.remove(name);
    }
    
    @ManagedOperation(description = "Clears all metrics in every monitor which is reachable through the agents in this context.")
    public void clearAllMetrics() {
        for (MonitorAgent agent : objectNameToAgentMap.values()) {
            for (Monitor m : agent) {
                m.clearMetrics();
            }
        }
    }
    
    @Override
    public synchronized void eventOccured(MonitoringEvent event) {
        if (event instanceof MonitorAddedEvent) {
            if (((MonitorAddedEvent) event).getAddedMonitor() instanceof WindowedMonitor) {
                WindowedMonitor tm = (WindowedMonitor) ((MonitorAddedEvent) event).getAddedMonitor();
                registerWindowedMonitor(tm);
            }
        }
        else if (event instanceof WindowedMonitorPollingIntervalChanged) {
            synchronized (windowMonitorMap) {
                WindowedMonitorPollingIntervalChanged intervalChangedEvent = (WindowedMonitorPollingIntervalChanged) event;
                WindowedMonitorPollingTask oldPollingTask = windowMonitorMap.get(intervalChangedEvent
                        .getOldWindowPeriod());
                if (oldPollingTask != null) {
                    oldPollingTask.removeWindowedMonitor(intervalChangedEvent.getModifiedMonitor());
                }
                registerWindowedMonitor(intervalChangedEvent.getModifiedMonitor());
            }
        }
        else if (event instanceof AgentRegistrationEvent) {
            AgentRegistrationEvent regEvent = (AgentRegistrationEvent) event;
            if (regEvent.isRegistration()) {
                registerAgent(regEvent.getObjectName(), regEvent.getAgent());
            } else {
                unregisterAgent(regEvent.getObjectName(), regEvent.getAgent());
            }
        }
    }
    
    @ManagedAttribute(description = "The monitor level of the context as a string")
    public synchronized void setMonitorLevel(String monitorLevelString) {
        if (monitorLevelString == null) {
            throw new IllegalArgumentException("monitorLevelString can not be null");
        }
        MonitorCategory levelToSet = null;
        try {
            levelToSet = MonitorCategory.valueOf(monitorLevelString);
        } catch (IllegalArgumentException iae) {
            StringBuilder sb = new StringBuilder();
            for (MonitorCategory level : MonitorCategory.values()) {
                sb.append(level.name());
                sb.append(' ');
            }
            throw new IllegalArgumentException(monitorLevelString + " is not recognised! Allowed values are: "
                    + sb.toString());
        }
        this.monitorLevel = levelToSet;
        for (MonitorAgent agent : objectNameToAgentMap.values()) {
            agent.setMonitorCategory(levelToSet);
        }
    }
    
    @ManagedAttribute(description = "The monitor level of the context as a string")
    public synchronized String getMonitorLevel() {
        return monitorLevel.name();
    }
    @Override
    public ObjectName getObjectName() throws MalformedObjectNameException {
        return new ObjectName("msjava.management:type=MonitoringManager");
    }
    
    private List<MonitorDescriptor> findConfiguration(ObjectName name) {
        List<MonitorDescriptor> descriptors = new ArrayList<MonitorDescriptor>();
        List<ObjectName> matchingObjectNamePatterns = findMatchingObjectNamePatterns(name);
        for (ObjectName matchingObjectNamePattern : matchingObjectNamePatterns) {
            descriptors.addAll(configuration.get(matchingObjectNamePattern));
        }
        if (configuration.containsKey(name)) {
            descriptors.addAll(configuration.get(name));
        }
        if (descriptors.size() > 0) {
            return mergeSameDescriptors(descriptors);
        } else {
            return descriptors;
        }
    }
    private List<ObjectName> findMatchingObjectNamePatterns(ObjectName name) {
        List<ObjectName> matchingObjectNamePatterns = new ArrayList<ObjectName>();
        for (ObjectName configurationObjectName : configuration.keySet()) {
            if (configurationObjectName.isPattern() && configurationObjectName.apply(name)) {
                matchingObjectNamePatterns.add(configurationObjectName);
            }
        }
        return matchingObjectNamePatterns;
    }
    private synchronized void applyConfigurationOverrides(ObjectName name, MonitorAgent agent) {
        if (configuration == null) {
            return;
        }
        List<MonitorDescriptor> conf = findConfiguration(name); 
        if (conf.size() > 0) {
            applyConfigurationOverrides(name, agent, conf);
        }
    }
    
    private synchronized void applyConfigurationOverrides(ObjectName name, MonitorAgent agent,
            List<MonitorDescriptor> conf) {
        Map<String, MonitorDescriptor> monitorIDtoDescriptorMap = new HashMap<String, MonitorDescriptor>();
        
        MonitorDescriptor wildcardDescriptor = null;
        for (MonitorDescriptor desc : conf) {
            if (isWildcardDescriptor(desc)) {
                if (wildcardDescriptor == null) {
                    wildcardDescriptor = desc;
                } else {
                    throw new IllegalStateException(
                            "It's a bug there's more than one wildcard merged descriptor. Please report to javahelp.");
                }
            } else {
                monitorIDtoDescriptorMap.put(desc.getMonitorID(), desc);
            }
        }
        for (Monitor m : agent) {
            MonitorDescriptor desc = monitorIDtoDescriptorMap.get(m.getDescriptor().getMonitorID());
            if (desc != null) {
                applyDescriptorOverrides(name, m, desc);
            } else if (wildcardDescriptor != null) {
                applyDescriptorOverrides(name, m, wildcardDescriptor);
            }
        }
    }
    private void applyDescriptorOverrides(ObjectName name, Monitor m, MonitorDescriptor desc) {
        MonitorDescriptor mergedDescriptor = m.getDescriptor().mergeDescriptors(desc);
        m.setDescriptor(mergedDescriptor);
        setDescriptorUsed(name, desc);
    }
    
    private boolean isWildcardDescriptor(MonitorDescriptor desc) {
        return "*".equals(desc.getMonitorID());
    }
    private void setDescriptorUsed(ObjectName name, MonitorDescriptor desc) {
        List<ObjectName> matchingObjectNamePatterns = findMatchingObjectNamePatterns(name);
        for (ObjectName matchingObjectNamePattern : matchingObjectNamePatterns) {
            removeUsedDescriptorFromUnusedConfiguration(matchingObjectNamePattern, desc);
        }
        if (configuration.containsKey(name)) {
            removeUsedDescriptorFromUnusedConfiguration(name, desc);
        }
    }
    private void removeUsedDescriptorFromUnusedConfiguration(ObjectName name, MonitorDescriptor desc) {
        List<MonitorDescriptor> unusedDescriptors = unusedConfiguration.get(name);
        if (unusedDescriptors != null) {
            unusedDescriptors.remove(desc);
            if (unusedDescriptors.size() == 0) {
                unusedConfiguration.remove(name);
            }
        }
    }
    
    @ManagedOperation(description = "Convenient method to tell, which configurations are not used.")
    public Map<ObjectName, List<String>> retriveUnUsedConfigurations() {
        Map<ObjectName, List<String>> result = new HashMap<ObjectName, List<String>>();
        for (Entry<ObjectName, List<MonitorDescriptor>> entry : unusedConfiguration.entrySet()) {
            List<String> stringRep = new ArrayList<String>();
            for (MonitorDescriptor desc : entry.getValue()) {
                stringRep.add(desc.toString());
            }
            result.put(entry.getKey(), stringRep);
        }
        return result;
    }
    
    public synchronized void setConfiguration(Map<String, List<MonitorDescriptor>> config)
            throws MalformedObjectNameException {
        if (config == null) {
            throw new IllegalArgumentException("Configuration can not be null");
        }
        this.configuration = new HashMap<ObjectName, List<MonitorDescriptor>>();
        for (Entry<String, List<MonitorDescriptor>> entry : config.entrySet()) {
            ObjectName name = new ObjectName(entry.getKey());
            List<MonitorDescriptor> descriptors = mergeSameDescriptors(entry.getValue());
            configuration.put(name, descriptors);
        }
        this.unusedConfiguration = new HashMap<ObjectName, List<MonitorDescriptor>>();
        for (Entry<ObjectName, List<MonitorDescriptor>> entry : this.configuration.entrySet()) {
            unusedConfiguration.put(entry.getKey(), new ArrayList<MonitorDescriptor>(entry.getValue()));
        }
        for (Entry<ObjectName, MonitorAgent> entry : objectNameToAgentMap.entrySet()) {
            applyConfigurationOverrides(entry.getKey(), entry.getValue());
        }
    }
    
    public synchronized void addMonitorConfigurator(MonitorConfigurator configurator) {
        monitorConfigurators.add(configurator);
        for (Entry<ObjectName, MonitorAgent> entry : objectNameToAgentMap.entrySet()) {
            applyMonitorConfigurator(configurator, entry.getKey(), entry.getValue());
        }
    }
    
    public synchronized void setMonitorConfigurators(List<MonitorConfigurator> configurators) {
        if(configurators == null) {
            throw new IllegalArgumentException("monitorConfigurators can not be null");
        }
        
        monitorConfigurators.clear();
        monitorConfigurators.addAll(configurators);
        
        for (Entry<ObjectName, MonitorAgent> entry : objectNameToAgentMap.entrySet()) {
            applyMonitorConfigurators(entry.getKey(), entry.getValue());
        }
    }
    private void applyMonitorConfigurators(ObjectName objectName, MonitorAgent agent) {
        for (MonitorConfigurator configurator : monitorConfigurators) {
            applyMonitorConfigurator(configurator, objectName, agent);
        }
        
    }
    private void applyMonitorConfigurator(MonitorConfigurator configurator, ObjectName objectName, MonitorAgent agent) {
        
        
        
        if(configurator instanceof FilteredMonitorConfigurator) {
            for (Monitor monitor : agent) {
                ((FilteredMonitorConfigurator) configurator).configure(objectName, monitor);
            }
        } else {
            for (Monitor monitor : agent) {
                configurator.configure(monitor);
            }
        }
    }
    
    static List<MonitorDescriptor> mergeSameDescriptors(List<MonitorDescriptor> configuration) {
        Map<String, MonitorDescriptor> idToDescriptorMap = new HashMap<String, MonitorDescriptor>();
        for (MonitorDescriptor desc : configuration) {
            String monitorID = desc.getMonitorID();
            MonitorDescriptor resultDescriptor = idToDescriptorMap.get(monitorID);
            if (resultDescriptor == null) {
                idToDescriptorMap.put(monitorID, desc);
            } else {
                MonitorDescriptor mergedDescriptor = resultDescriptor.mergeDescriptors(desc);
                idToDescriptorMap.put(monitorID, mergedDescriptor);
            }
        }
        return new ArrayList<MonitorDescriptor>(idToDescriptorMap.values());
    }
    
    @Override
    public long getCurrentTime() {
        return clock.getCurrentTime();
    }
    
    @ManagedAttribute(description = "The unit of measurement used", currencyTimeLimit = Integer.MAX_VALUE)
    @Override
    public String getUnitOfMeasurement() {
        return clock.getUnitOfMeasurement();
    }
    
    public void setClockType(Clock.Type type) {
        if (type == Clock.Type.MILLISEC) {
            clock = new MilliSecondClock();
        } else if (type == Clock.Type.NANOSEC) {
            clock = new NanoSecondClock();
        } else {
            throw new IllegalStateException("Unknown clock type" + type.name());
        }
    }
    
    class WindowedMonitorPollingTask implements Runnable {
        List<WindowedMonitor> windowedMonitors = new CopyOnWriteArrayList<WindowedMonitor>();
        @Override
        public void run() {
            for (WindowedMonitor tm : windowedMonitors) {
                tm.windowExpired();
            }
        }
        
        public void addWindowedMonitor(WindowedMonitor windowedMonitor) {
            windowedMonitors.add(windowedMonitor);
        }
        
        public boolean removeWindowedMonitor(WindowedMonitor windowedMonitor) {
            return windowedMonitors.remove(windowedMonitor);
        }
    }
}
