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

package msjava.base.admin;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import msjava.base.admin.internal.MSAdminCommandStoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
@Deprecated
public class MSAdminCommandStore implements BeanPostProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(MSAdminCommandStore.class);
    
    
    
    
    
    
    
    
    
    
    
    
    
    protected Map<MSAdminCommandStoreItem, MSAdminCommandStoreItem> _commandsAndAliases = new ConcurrentHashMap<MSAdminCommandStoreItem, MSAdminCommandStoreItem>();
    private final CopyOnWriteArrayList<MSAdminCommandStoreListener> _listeners = new CopyOnWriteArrayList<MSAdminCommandStoreListener>();
    
    
    private static final MSAdminCommandStore INSTANCE = new MSAdminCommandStore();
    public void addListener(MSAdminCommandStoreListener listener_) {
        _listeners.add(listener_);
    }
    public void removeListener(MSAdminCommandStoreListener listener_) {
        _listeners.remove(listener_);
    }
    private static interface ListenerCallback {
        public void forEach(MSAdminCommandStoreListener listener);
    }
    private void fireListeners(ListenerCallback callback) {
        for (MSAdminCommandStoreListener listener : _listeners) {
            try {
                callback.forEach(listener);
            } catch (Throwable e) {
                LOGGER.error("Caught exception running MSAdminCommandStore listener", e);
            }
        }
    }
    
    public static MSAdminCommandStore getInstance() {
        return INSTANCE;
    }
    
    public Collection<? extends MSAdminCommandStoreItem> getAllCommandsAndAliases(boolean sorted_) {
        
        List<? extends MSAdminCommandStoreItem> defensiveCopy = defensiveCopy();
        if (sorted_) {
            Collections.sort(defensiveCopy, MSAdminCommandStoreItemComparator.instance());
        }
        return defensiveCopy;
    }
    
    @SuppressWarnings("unchecked")
	public Collection<MSAdminCommand> getAllCommands(boolean sorted_) {
        
        List<? extends MSAdminCommandStoreItem> defensiveCopy = defensiveCopy();
        
        removeAllButThisType(MSAdminCommand.class, defensiveCopy);
        
        if (sorted_) {
            Collections.sort(defensiveCopy, MSAdminCommandStoreItemComparator.instance());
        }
        return (Collection<MSAdminCommand>) defensiveCopy;
    }
    
    @SuppressWarnings("unchecked")
	public Collection<MSAdminCommandAlias> getAllAliases(boolean sorted_) {
        
        List<? extends MSAdminCommandStoreItem> defensiveCopy = defensiveCopy();
        
        removeAllButThisType(MSAdminCommandAlias.class, defensiveCopy);
        
        if (sorted_) {
            Collections.sort(defensiveCopy, MSAdminCommandStoreItemComparator.instance());
        }
        return (Collection<MSAdminCommandAlias>) defensiveCopy;
    }
    
    public void add(final MSAdminCommand command_) throws IllegalArgumentException {
        if (command_ != null) {
            
            MSAdminUtil.throwIfInvalidCommand(command_);
            if (MSAdminUtil.countNumberOfConstArgs(command_) > 0) {
                throw new IllegalArgumentException("Command supplied contains const args");
            }
            _commandsAndAliases.put(command_, command_);
            fireListeners(new ListenerCallback() {
                public void forEach(MSAdminCommandStoreListener listener_) {
                    listener_.commandRegistered(command_);
                }
            });
        }
    }
    
    public void add(MSAdminCommand command_, String[] constArgs_) throws IllegalArgumentException {
        if (command_ == null) {
            return;
        }
        
        MSAdminUtil.throwIfInvalidCommand(command_);
        
        
        
        
        
        
        MSAdminCommand commandFromStore = getCommand(command_.getNamespace(), command_.getName());
        
        if (commandFromStore instanceof MSAdminConstArgsBridgeCommand) {
            MSAdminConstArgsBridgeCommand constArgsBridgeCommand = (MSAdminConstArgsBridgeCommand) commandFromStore;
            constArgsBridgeCommand.registerConstArgsCommand(constArgs_, command_);
        }
        
        else if (commandFromStore == null) {
            MSAdminConstArgsBridgeCommand constArgsBridgeCommand = new MSAdminConstArgsBridgeCommand(command_
                    .getNamespace(), command_.getName(), null, null, null);
            constArgsBridgeCommand.registerConstArgsCommand(constArgs_, command_);
            add(constArgsBridgeCommand);
        }
        
        else {
            MSAdminConstArgsBridgeCommand constArgsBridgeCommand = new MSAdminConstArgsBridgeCommand(command_
                    .getNamespace(), command_.getName(), command_.getShortDescription(), command_.getLongDescription(),
                    command_.getConfirmation());
            constArgsBridgeCommand.registerConstArgsCommand(constArgs_, command_);
            constArgsBridgeCommand.setDefaultCommand(commandFromStore);
            add(constArgsBridgeCommand);
        }
    }
    
    public void add(final MSAdminCommandAlias alias_) {
        if (alias_ != null) {
            _commandsAndAliases.put(alias_, alias_);
            fireListeners(new ListenerCallback() {
                public void forEach(MSAdminCommandStoreListener listener_) {
                    listener_.aliasRegistered(alias_);
                }
            });
        }
    }
    
    public boolean remove(MSAdminCommandStoreItem command_) {
        return removeItem(command_) != null;
    }
    
    public boolean remove(String namespace_, String name_) {
        
        if (namespace_ == null || name_ == null) {
            return false;
        }
        
        
        
        MSAdminCommandStoreItem dummyItem = new MSAdminCommandStoreItemBaseImpl(namespace_, name_);
        return removeItem(dummyItem) != null;
    }
    
    public boolean remove(MSAdminCommand command_, String[] constArgs_) {
        return remove(command_.getNamespace(), command_.getName(), constArgs_);
    }
    
    public boolean remove(String namespace_, String name_, String[] constArgs_) {
        MSAdminCommand commandFromStore = getCommand(namespace_, name_);
        if (!(commandFromStore instanceof MSAdminConstArgsBridgeCommand)) {
            return false;
        }
        MSAdminConstArgsBridgeCommand constArgsBridgeCommand = (MSAdminConstArgsBridgeCommand) commandFromStore;
        return constArgsBridgeCommand.removeConstArgsCommand(constArgs_);
    }
    
    public MSAdminCommandAlias getAlias(String nameSpace_, String name_) {
        if (nameSpace_ == null || name_ == null) {
            return null;
        }
        MSAdminCommandStoreItem item = getFromSet(new MSAdminCommandStoreItemBaseImpl(nameSpace_, name_));
        if (!(item instanceof MSAdminCommandAlias)) {
            return null;
        }
        return (MSAdminCommandAlias) item;
    }
    
    public MSAdminCommand getCommand(String nameSpace_, String name_) {
        if (nameSpace_ == null || name_ == null) {
            return null;
        }
        MSAdminCommandStoreItem item = getFromSet(new MSAdminCommandStoreItemBaseImpl(nameSpace_, name_));
        if (!(item instanceof MSAdminCommand)) {
            return null;
        }
        return (MSAdminCommand) item;
    }
    
    public MSAdminCommand resolveToCommand(String nameSpace_, String name_) throws MSAdminDanglingAliasException,
            MSAdminCircularAliasException {
        if (nameSpace_ == null || name_ == null) {
            return null;
        }
        MSAdminCommandStoreItem currentItem = getFromSet(new MSAdminCommandStoreItemBaseImpl(nameSpace_, name_));
        if (currentItem == null) {
            return null;
        }
        Vector<MSAdminCommandAlias> visitedAliases = new Vector<MSAdminCommandAlias>();
        while (true) {
            if (currentItem instanceof MSAdminCommand) {
                return (MSAdminCommand) currentItem;
            } else if (!(currentItem instanceof MSAdminCommandAlias)) {
                return null;
            } else {
                
                MSAdminCommandAlias alias = (MSAdminCommandAlias) currentItem;
                if (visitedAliases.contains(alias)) {
                    throw new MSAdminCircularAliasException(alias, visitedAliases);
                }
                visitedAliases.add(alias);
                currentItem = getFromSet(new MSAdminCommandStoreItemBaseImpl(alias.getTargetNamespace(), alias
                        .getTargetName()));
                if (currentItem == null) {
                    throw new MSAdminDanglingAliasException(alias);
                }
            }
        }
    }
    
    protected Object removeItem(MSAdminCommandStoreItem item_) {
        if (item_ == null) {
            return null;
        }
        final MSAdminCommandStoreItem removed = _commandsAndAliases.remove(item_);
        fireListeners(new ListenerCallback() {
            public void forEach(MSAdminCommandStoreListener listener_) {
                if (removed instanceof MSAdminCommand) {
                    listener_.commandUnregistered((MSAdminCommand) removed);
                } else if (removed instanceof MSAdminCommandAlias) {
                    listener_.aliasUnregistered((MSAdminCommandAlias) removed);
                }
            }
        });
        return removed;
    }
    
    protected MSAdminCommandStoreItem getFromSet(MSAdminCommandStoreItem item_) {
        if (item_ == null) {
            return null;
        }
        return _commandsAndAliases.get(item_);
    }
    
    protected List<? extends MSAdminCommandStoreItem> defensiveCopy() {
        return new ArrayList<MSAdminCommandStoreItem>(_commandsAndAliases.keySet());
    }
    
    protected static void removeAllButThisType(Class<?> type_, Collection<?> collection_) {
        for (Iterator<?> i = collection_.iterator(); i.hasNext();) {
            Object item = i.next();
            if (!type_.isAssignableFrom(item.getClass())) {
                i.remove();
            }
        }
    }
    
    public static class MSAdminCommandStoreItemComparator implements Comparator<MSAdminCommandStoreItem> {
        protected final static MSAdminCommandStoreItemComparator _instance = new MSAdminCommandStoreItemComparator();
        
        public int compare(MSAdminCommandStoreItem o1_, MSAdminCommandStoreItem o2_) {
            MSAdminCommandStoreItem item1 = o1_;
            MSAdminCommandStoreItem item2 = o2_;
            int compareNamespaces = item1.getNamespace().compareTo(item2.getNamespace());
            if (compareNamespaces == 0) {
                return item1.getName().compareTo(item2.getName());
            }
            return compareNamespaces;
        }
        
        public static Comparator<MSAdminCommandStoreItem> instance() {
            return _instance;
        }
    }
	@Override
	public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
		return bean;
	}
	@Override
	public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
		if (bean instanceof MSAdminCommandStoreAware) {
			((MSAdminCommandStoreAware)bean).setMSAdminCommandStore(this);
		}
		return bean;
	}
}