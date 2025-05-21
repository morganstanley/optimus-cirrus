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
package msjava.base.event.publication.registry;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import msjava.base.annotation.Experimental;
import msjava.base.event.publication.Event;
import msjava.base.event.publication.EventPublisher;
import msjava.base.event.publication.StatefulEventPublisher;
import msjava.base.event.publication.direct.DirectToEndpointEventPublisher;
import msjava.base.event.publication.integrations.Consumer;
import msjava.base.event.publication.logging.LoggingEndpoint;
import msjava.base.spring.lifecycle.BeanState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException.Missing;
import com.typesafe.config.ConfigException.WrongType;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
@Experimental
public class EventPublisherRegistry {
	
	
	public static final String SYS_PROP_APPLICATION_DEFAULT_CONFIGURATION = "msjava.base.event.publication.registry.configuration";
	
	private static final Logger logger = LoggerFactory.getLogger(EventPublisherRegistry.class);
	
	
	public static EventPublisherRegistry getDefault() {
		return DefaultInstance.INSTANCE.registry;
	}
	
	static {
		getDefault(); 
	}
	private final BeanState state = new BeanState();
	
	@VisibleForTesting
	final ConcurrentHashMap<String, EventPublisherBuilder> builders = new ConcurrentHashMap<>(); 
	
	private final Map<StatefulEventPublisher, Void> statefulPublishers = new IdentityHashMap<>();
	
	
	private final Config config;
	
	
	
	public EventPublisherRegistry(Config config) {
		this.config = config;
		processBuilders(config.getObject("msjava.base.event.publication.builders"));
		if(config.getBoolean("msjava.base.event.publication.shutdownHook")) {
			installShutdownHook();
		}
		state.initializeIfNotInitialized();
		state.startIfNotRunning();
	}
	
	public static void shutdownDefault() throws InterruptedException {
		getDefault().shutdown(false, null);
	}
	
	
	public void shutdown() throws InterruptedException {
		shutdown(false, null);
	}
	
	
	public void installShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread(){
			@Override
			public void run() {
				try {
					shutdown(true, null);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
	}
	
	
	public void shutdown(final boolean shutdownHook, final Consumer<Event> consumer) throws InterruptedException {
		final AtomicReference<InterruptedException> interruptedException = new AtomicReference<>();
		state.stopIfRunning(new Runnable() {
			
			@Override
			public void run() {
				try {
					synchronized(statefulPublishers) {
						for (StatefulEventPublisher p: statefulPublishers.keySet()) {
							
							p.shutdown(shutdownHook, consumer);
						}
						statefulPublishers.clear();
					}
					builders.clear();
				}
				catch(InterruptedException e) {
					interruptedException.set(e);
				}
			}
			
		});
		if (interruptedException.get() != null) {
			throw interruptedException.get();
		}
	}
	
	private void processBuilders(ConfigObject builders) {
		for (Entry<String, ConfigValue> entry: builders.entrySet()) {
			processBuilder(entry.getValue(), entry.getKey());
		}
	}
	
    private void processBuilder(ConfigValue v, String builderName) {
        try {
            if (v.valueType() == ConfigValueType.OBJECT) {
                ConfigObject o = (ConfigObject) v;
                
                Class<?> builderClass = findClass((String) o.get("className").unwrapped(), EventPublisherBuilder.class,
                        Thread.currentThread().getContextClassLoader(), EventPublisherBuilder.class.getClassLoader());
                EventPublisherBuilder b = (EventPublisherBuilder) builderClass.getConstructor().newInstance();
                b.init(o.get("config"));
                this.builders.put(builderName, b);
            }
        } catch (Exception e) {
            logger.error("exception occurred while processing config", e);
        }
    }
    private static Class<?> findClass(String className, Class<?> assignableTo, ClassLoader... loaders)
            throws ClassNotFoundException {
        for (ClassLoader clldr : loaders) {
            try {
                Class<?> klass = clldr.loadClass(className);
                if (assignableTo.isAssignableFrom(klass)) {
                    logger.debug("Loaded {} using {}", className, clldr);
                    return klass;
                }
            } catch (ClassNotFoundException e) {
                
            }
        }
        throw new ClassNotFoundException(className + " assignable to " + assignableTo.getName());
    }
	
	public static EventPublisher lookup(Class<?> c) {
		return getDefault().doLookup(c.getName());
	}
	
	public static EventPublisher lookup(String path) {
		return getDefault().doLookup(path);
	}
	
	@VisibleForTesting
	public EventPublisher doLookup(String path) {
		if (state.isStopped()) {
			throw new IllegalStateException("Already shutdown");
		}
		try {
			EventPublisher p = lookupBuilder(path).build(path);
			if (p instanceof StatefulEventPublisher) {
				synchronized(statefulPublishers) {
					statefulPublishers.put((StatefulEventPublisher)p,  null);
				}
			}
			return p;
		}
		catch (RuntimeException re) {
			
			logger.debug("cannot create event publisher using root builder - defaulting to logging impl", re);
			return new DirectToEndpointEventPublisher(
				new LoggingEndpoint(LoggerFactory.getLogger(path), null)
			);
		}
	}
	
	private EventPublisherBuilder lookupBuilder(String path) {
		try {
			return builders.get(
				config.getString("msjava.base.event.publication.publisherMappings." + path));
		}
		catch (Missing | WrongType e) {
			try {
				return lookupBuilder(path.substring(0, path.lastIndexOf(".")));
			}
			catch(IndexOutOfBoundsException ex) {
				
				return builders.get("root");
			}
		}
	}
	
	public Config config() {
		return config;
	}
	private enum DefaultInstance {
		INSTANCE;
		
		private EventPublisherRegistry registry;
		
		private DefaultInstance() {
			Config conf = ConfigFactory.empty();
			for (String r: new String[]{"reference.conf", System.getProperty(SYS_PROP_APPLICATION_DEFAULT_CONFIGURATION, "eventPublisher.conf")}) {
				try {
					conf = ConfigFactory.load(r)
						.withFallback(conf);
				}
				catch(RuntimeException e) {
					throw new RuntimeException("exception caught while loading from " + r, e);
				}
			}
			conf = conf.resolve();
			logger.debug("Resolved typesafe conf {}", conf.root().render());
			registry = new EventPublisherRegistry(conf);
		}
	}
	
}
