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

package msjava.tools.util;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.WeakHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
@SuppressWarnings({"rawtypes", "unchecked"})
public class MSCacheMap extends AbstractMap
{
  private static final Logger LOGGER = LoggerFactory.getLogger(MSCacheMap.class);
  
  LinkedList _listeners;
  LinkedList _listenersGlobal;
  SoftCacheInfo _cache; 
  final ReplacementPolicy _policy;
  int _maxSize;
  long _unusedTimeout;
  long _staleTimeout;
  boolean _garbageCollectable;
  Set _entrySet;
  int _sequenceNumber;
  final boolean _usingTimeouts;
  
  
  static LinkedList _cachesWithTimers = new LinkedList();
  static Thread _timer = null;
  static long _nextWakeup;
  static final ReferenceQueue _cachesClearedQueue = new ReferenceQueue();
  
  @Deprecated
static final Map _cachesFromRefs = new WeakHashMap();
  
  public interface AccessInfoKey
  {
    public Object makeValue();
  }
  
  public interface CacheListener
  {
    public void entryCastOut( CacheEntry entry_ );
    public void entryUnusedTimeout( CacheEntry entry_ );
    public void entryStaleTimeout( CacheEntry entry_ );
  }
  
  @Deprecated
public interface GlobalCacheListener
  {
  
    @Deprecated
    public void cacheClearedByGC();
  }
  public interface ReplacementPolicy
  {
    
    public abstract int compare( CacheEntry a_, CacheEntry b_ );
  }
  
  public static class LRUPolicy implements ReplacementPolicy
  {
    public int compare( CacheEntry a_, CacheEntry b_ )
    {
      return ( int ) (a_.lastAccess() - b_.lastAccess());
    }
  }
  
  public class CacheEntry implements Map.Entry
  {
    int _heapLocation;
    LeastRecentListNode _lrrNode; 
    LeastRecentListNode _lrwNode; 
    protected Object _key;
    protected Object _value;
    protected long _lastRead;
    protected long _lastWrite;
    public CacheEntry( Object key_, Object value_, long lastRead_, long lastWrite_ )
    {
      _key = key_;
      _value = value_;
      _lastRead = lastRead_;
      _lastWrite = lastWrite_;
    }
    public final long lastRead()
    {
      return _lastRead;
    }
    public final long lastWrite()
    {
      return _lastWrite;
    }
    public final long lastAccess()
    {
      return Math.max( _lastRead, _lastWrite );
    }
    public final Object getKey()
    {
      return _key;
    }
    public final Object getValue()
    {
      return _value;
    }
    public final Object setValue( Object newValue_ )
    {
      Object oldValue = _value;
      _value = newValue_;
      return oldValue;
    }
    @Override
    public String toString()
    {
      return _value.toString();
    }
  }
  
  public MSCacheMap( ReplacementPolicy policy_, int maxSize_, long unusedTimeoutSeconds_,
      long staleTimeoutSeconds_ )
  {
    _policy = policy_;
    _maxSize = maxSize_;
    _unusedTimeout = unusedTimeoutSeconds_ * 1000;
    _staleTimeout = staleTimeoutSeconds_ * 1000;
    _usingTimeouts = (unusedTimeoutSeconds_ > 0 || staleTimeoutSeconds_ > 0);
    init( true );
    garbageCollectable( false );
  }
  
  public MSCacheMap( int maxSize_, long unusedTimeoutSeconds_, long staleTimeoutSeconds_ )
  {
    this( new LRUPolicy(), maxSize_, unusedTimeoutSeconds_, staleTimeoutSeconds_ );
  }
  
  public synchronized void garbageCollectable( boolean gc_ )
  {
    _garbageCollectable = gc_;
    _cache.garbageCollectable( gc_ );
    if( gc_ && size() > 0 )
    {
      startTimers( this );
    }
  }
  
  public boolean garbageCollectable()
  {
    return _cache.garbageCollectable();
  }
  CacheInfo init( boolean gc_ )
  {
    
    
    Heap heap = new Heap( (_maxSize == 0) ? 50 : _maxSize, new Comparator() {
      public int compare( Object entryA_, Object entryB_ )
      {
        return _policy.compare( ( CacheEntry ) entryA_, ( CacheEntry ) entryB_ );
      }
    } );
    
    HashMap map = new HashMap( (_maxSize == 0) ? 100 : (_maxSize << 1) );
    CacheInfo cache = new CacheInfo( map, heap );
    _cache = new SoftCacheInfo( cache, gc_ );
    
    
    
    
    
    return cache;
  }
  
  public synchronized void addCacheListener( CacheListener listener_ )
  {
    if( _listeners == null )
    {
      _listeners = new LinkedList();
    }
    _listeners.add( listener_ );
  }
  
  public synchronized void removeCacheListener( CacheListener listener_ )
  {
    _listeners.remove( listener_ );
    if( _listeners.size() == 0 )
    {
      _listeners = null;
    }
  }
  
  @Deprecated
public synchronized void addGlobalCacheListener( GlobalCacheListener listener_ )
  {
    if( _listenersGlobal == null )
    {
      _listenersGlobal = new LinkedList();
    }
    _listenersGlobal.add( listener_ );
  }
  
  @Deprecated
public synchronized void removeGlobalCacheListener( GlobalCacheListener listener_ )
  {
    _listenersGlobal.remove( listener_ );
    if( _listenersGlobal.size() == 0 )
    {
      _listenersGlobal = null;
    }
  }
  
  @Override
public boolean containsKey( Object key_ )
  {
    return (_usingTimeouts) ? containsKeySync( key_ ) : containsKeyNoSync( key_ );
  }
  synchronized final boolean containsKeySync( Object key_ )
  {
    return containsKeyNoSync( key_ );
  }
  boolean containsKeyNoSync( Object key_ )
  {
    HashMap map = _cache.map();
    return map != null && map.containsKey( key_ );
  }
  
  @Override
public final Object get( Object key_ )
  {
    return (_usingTimeouts) ? getSync( key_ ) : getNoSync( key_ );
  }
  synchronized final Object getSync( Object key_ )
  {
    return getNoSync( key_ );
  }
  Object getNoSync( Object key_ )
  {
    CacheInfo cache = _cache.info();
    if( cache == null )
    {
      return null;
    }
    CacheEntry entry = ( CacheEntry ) cache.map().get( key_ );
    if( entry == null )
    {
      
      Object value = null;
      if( key_ instanceof AccessInfoKey )
      {
        value = (( AccessInfoKey ) key_).makeValue();
        putNoSync( key_, value );
      }
      return value;
    }
    entry._lastRead = currentTimeMillis( this );
    
    cache.heap().heapify( entry );
    if( _usingTimeouts )
    {
      cache.lrrList().markMostRecent( entry );
    }
    return entry._value;
  }
  
  @Override
public final Object put( Object key_, Object value_ )
  {
    return (_usingTimeouts) ? putSync( key_, value_ ) : putNoSync( key_, value_ );
  }
  synchronized final Object putSync( Object key_, Object value_ )
  {
    return putNoSync( key_, value_ );
  }
  Object putNoSync( Object key_, Object value_ )
  {
    CacheInfo cache = _cache.info();
    if( cache == null )
    {
      
      cache = init( garbageCollectable() );
    }
    HashMap map = cache.map();
    CacheEntry oldEntry = ( CacheEntry ) map.get( key_ );
    if( oldEntry == null )
    {
      Heap heap = cache.heap();
      LeastRecentList lrrList = cache.lrrList();
      LeastRecentList lrwList = cache.lrwList();
      
      if( _maxSize != 0 && map.size() >= _maxSize )
      {
        
        CacheEntry toCastOut = heap.peek();
        heap.extract();
        if( map.remove( toCastOut.getKey() ) == null )
          throw new IllegalArgumentException( "Could not find entry in map" );
        if( _usingTimeouts )
        {
          lrrList.remove( toCastOut );
          lrwList.remove( toCastOut );
        }
        fireEntryCastOut( toCastOut );
      }
      long now = currentTimeMillis( this );
      CacheEntry entry = newCacheEntry( key_, value_, now, now );
      Object rv = map.put( key_, entry );
      heap.insert( entry );
      if( _usingTimeouts )
      {
        lrrList.insert( entry );
        lrwList.insert( entry );
      }
      
      
      
      
      if( map.size() == 1 && (_usingTimeouts || _garbageCollectable) )
      {
        startTimers( this );
      }
      return rv;
    }
    else
    
    {
      oldEntry._lastWrite = currentTimeMillis( this );
      cache.heap().heapify( oldEntry );
      if( _usingTimeouts )
      {
        cache.lrwList().markMostRecent( oldEntry );
      }
      Object oldValue = oldEntry._value;
      oldEntry._value = value_;
      return oldValue;
    }
  }
  
  @Override
public final Object remove( Object key_ )
  {
    return (_usingTimeouts) ? removeSync( key_ ) : removeNoSync( key_ );
  }
  synchronized final Object removeSync( Object key_ )
  {
    return removeNoSync( key_ );
  }
  Object removeNoSync( Object key_ )
  {
    CacheInfo cache = _cache.info();
    if( cache == null )
    {
      return null;
    }
    CacheEntry entry = ( CacheEntry ) cache.map().remove( key_ );
    if( entry != null )
    {
      cache.heap().remove( entry );
      if( _usingTimeouts )
      {
        cache.lrrList().remove( entry );
        cache.lrwList().remove( entry );
      }
      return entry.getValue();
    }
    return null;
  }
  
  @Override
public int size()
  {
    return (_usingTimeouts) ? sizeSync() : sizeNoSync();
  }
  final synchronized int sizeSync()
  {
    return sizeNoSync();
  }
  int sizeNoSync()
  {
    HashMap map = _cache.map();
    return (map == null) ? 0 : map.size();
  }
  
  @Override
public void clear()
  {
    if( _usingTimeouts )
      clearSync();
    else
      clearNoSync();
  }
  final synchronized void clearSync()
  {
    clearNoSync();
  }
  void clearNoSync()
  {
    synchronized( this )
    {
      CacheInfo cache = _cache.info();
      if( cache != null )
      {
        cache.map().clear();
        cache.heap().clear();
        if( _usingTimeouts )
        {
          cache.lrrList().clear();
          cache.lrwList().clear();
        }
      }
    }
  }
  
  @Override
public boolean isEmpty()
  {
    return (_usingTimeouts) ? isEmptySync() : isEmptyNoSync();
  }
  final synchronized boolean isEmptySync()
  {
    return isEmptyNoSync();
  }
  boolean isEmptyNoSync()
  {
    HashMap map = _cache.map();
    return (map == null || map.isEmpty());
  }
  
  @Override
public void putAll( Map other_ )
  {
    if( _usingTimeouts )
      putAllSync( other_ );
    else
      putAllNoSync( other_ );
  }
  final synchronized void putAllSync( Map other_ )
  {
    putAllNoSync( other_ );
  }
  void putAllNoSync( Map other_ )
  {
    Iterator i = other_.entrySet().iterator();
    while( i.hasNext() )
    {
      Map.Entry e = ( Map.Entry ) i.next();
      put( e.getKey(), e.getValue() );
    }
  }
  
  
  
  
  @Override
public boolean containsValue( Object value_ )
  {
    return (_usingTimeouts) ? containsValueSync( value_ ) : containsValueNoSync( value_ );
  }
  final synchronized boolean containsValueSync( Object value_ )
  {
    return containsValueNoSync( value_ );
  }
  final synchronized boolean containsValueNoSync( Object value_ )
  {
    return super.containsValue( value_ );
  }
  
  @Override
public Set keySet()
  {
    return (_usingTimeouts) ? keySetSync() : keySetNoSync();
  }
  final synchronized Set keySetSync()
  {
    return keySetNoSync();
  }
  final Set keySetNoSync()
  {
    return super.keySet();
  }
  
  @Override
public Collection values()
  {
    return (_usingTimeouts) ? valuesSync() : valuesNoSync();
  }
  final synchronized Collection valuesSync()
  {
    return valuesNoSync();
  }
  final Collection valuesNoSync()
  {
    return super.values();
  }
  
  @Override
  public boolean equals(Object o) {
    
    if ( o instanceof Map ) {
      return (_usingTimeouts) ? equalsSync(o) : equalsNoSync(o);
    } else {
      return false;
    }
  }
  final synchronized boolean equalsSync( Object o_ )
  {
    return equalsNoSync( o_ );
  }
  final boolean equalsNoSync( Object o_ )
  {
    return super.equals( o_ );
  }
  
  @Override
public int hashCode()
  {
    return (_usingTimeouts) ? hashCodeSync() : hashCodeNoSync();
  }
  final synchronized int hashCodeSync()
  {
    return hashCodeNoSync();
  }
  final int hashCodeNoSync()
  {
    return super.hashCode();
  }
  
  @Override
public String toString()
  {
    return (_usingTimeouts) ? toStringSync() : toStringNoSync();
  }
  final synchronized String toStringSync()
  {
    return toStringNoSync();
  }
  final String toStringNoSync()
  {
    return super.toString();
  }
  @Override
public Set entrySet()
  {
    return (_usingTimeouts) ? entrySetSync() : entrySetNoSync();
  }
  synchronized final Set entrySetSync()
  {
    return entrySetNoSync();
  }
  Set entrySetNoSync()
  {
    if( _entrySet == null )
    {
      _entrySet = new AbstractSet() {
        @Override
        public Iterator iterator()
        {
          return getCacheIterator();
        }
        @Override
        public boolean contains( Object o_ )
        {
          if( !(o_ instanceof Map.Entry) )
            return false;
          HashMap map = _cache.map();
          return map != null && map.containsKey( (( Map.Entry ) o_).getKey() );
        }
        @Override
        public boolean remove( Object o_ )
        {
          if( !(o_ instanceof Map.Entry) )
            return false;
          return MSCacheMap.this.remove( (( Map.Entry ) o_).getKey() ) != null;
        }
        @Override
        public int size()
        {
          HashMap map = _cache.map();
          return (map == null) ? 0 : map.size();
        }
        @Override
        public void clear()
        {
          MSCacheMap.this.clear();
        }
      };
    }
    return _entrySet;
  }
  
  protected CacheEntry newCacheEntry( Object key_, Object value_, long lastRead_, long lastWrite_ )
  {
    return new CacheEntry( key_, value_, lastRead_, lastWrite_ );
  }
  void fireEntryCastOut( CacheEntry e_ )
  {
    if( _listeners == null )
      return;
    for( Iterator i = _listeners.iterator(); i.hasNext(); )
    {
      try
      {
        (( CacheListener ) i.next()).entryCastOut( e_ );
      }
      catch( Throwable e )
      {
        if (LOGGER.isInfoEnabled())
        {
          LOGGER.info("Exception in entryCastOut()", e);
        }
      }
    }
  }
  void fireEntryUnusedTimeout( CacheEntry e_ )
  {
    if( _listeners == null )
      return;
    for( Iterator i = _listeners.iterator(); i.hasNext(); )
    {
      try
      {
        (( CacheListener ) i.next()).entryUnusedTimeout( e_ );
      }
      catch( Throwable e )
      {
        if (LOGGER.isInfoEnabled())
        {
          LOGGER.info("Exception in entryUnusedTimeout()", e);
        }
      }
    }
  }
  void fireEntryStaleTimeout( CacheEntry e_ )
  {
    if( _listeners == null )
      return;
    for( Iterator i = _listeners.iterator(); i.hasNext(); )
    {
      try
      {
        (( CacheListener ) i.next()).entryStaleTimeout( e_ );
      }
      catch( Throwable e )
      {
        if (LOGGER.isInfoEnabled())
        {
          LOGGER.info("Exception in entryStaleTimeout()", e);
        }
      }
    }
  }
  
  @Deprecated
void fireCacheClearedByGC()
  {
    if( _listenersGlobal == null )
      return;
    for( Iterator i = _listenersGlobal.iterator(); i.hasNext(); )
    {
      (( GlobalCacheListener ) i.next()).cacheClearedByGC();
    }
  }
  static EmptyCacheIterator _emptyCacheIterator = new EmptyCacheIterator();
  static class EmptyCacheIterator implements Iterator
  {
    EmptyCacheIterator()
    {
    }
    public boolean hasNext()
    {
      return false;
    }
    public Object next()
    {
      throw new NoSuchElementException();
    }
    public void remove()
    {
      throw new IllegalStateException();
    }
  }
  class CacheIterator implements Iterator
  {
    Iterator _iter;
    CacheEntry _current;
    CacheIterator( Map underlying_ )
    {
      _iter = underlying_.entrySet().iterator();
    }
    public boolean hasNext()
    {
      return _iter.hasNext();
    }
    public Object next()
    {
      return _current = ( CacheEntry ) (( Map.Entry ) _iter.next()).getValue();
    }
    public void remove()
    {
      _iter.remove();
      CacheInfo cache = _cache.info();
      if( cache != null )
      {
        cache.heap().remove( _current );
        cache.lrrList().remove( _current );
        cache.lrwList().remove( _current );
      }
    }
  }
  Iterator getCacheIterator()
  {
    HashMap map = _cache.map();
    if( map == null || map.size() == 0 )
    {
      return _emptyCacheIterator;
    }
    else
    {
      return new CacheIterator( map );
    }
  }
  
  static long currentTimeMillis( MSCacheMap map_ )
  {
    
    
    
    return (map_ == null || map_._usingTimeouts)
        ? System.currentTimeMillis()
        : ++map_._sequenceNumber;
  }
  static synchronized void startTimers( MSCacheMap map_ )
  {
    _cachesWithTimers.add( new WeakReference( map_, _cachesClearedQueue ) );
    if( _timer == null )
    {
      startTimerThread();
    }
    else
    {
      cleanupNeededAt( map_.runCleanup( null ) );
      
      
      new SoftReference( new Object(), _cachesClearedQueue ).enqueue();
    }
  }
  static synchronized void resetWakeupTime()
  {
    _nextWakeup = Long.MAX_VALUE;
  }
  static synchronized void cleanupNeededAt( long cleanupTime_ )
  {
    _nextWakeup = Math.min( _nextWakeup, cleanupTime_ );
  }
  static synchronized long timeTillWakeup()
  {
    return Math.max( 1000, _nextWakeup - currentTimeMillis( null ) );
  }
  static void startTimerThread()
  {
    
    _timer = new Thread( new Runnable() {
      public void run()
      {
        cleanupLoop();
        _timer = null;
        
      }
    } );
    _timer.setName( "MSCacheMap timer thread" );
    _timer.setDaemon( true );
    
    
    
    
    
    _timer.setPriority( Thread.MAX_PRIORITY );
    _timer.start();
  }
  
  static void cleanupLoop()
  {
    try
    {
      while( true ) 
      {
        resetWakeupTime();
        LinkedList cachesProcessed = new LinkedList();
        LinkedList cachesToProcess;
        try
        {
          while( true )
          {
            
            synchronized( MSCacheMap.class )
            {
              if( _cachesWithTimers.size() == 0 )
              {
                break;
              }
              cachesToProcess = _cachesWithTimers;
              _cachesWithTimers = new LinkedList();
            }
            for( Iterator i = cachesToProcess.iterator(); i.hasNext(); )
            {
              MSCacheMap cache = ( MSCacheMap ) (( WeakReference ) i.next()).get();
              if( cache == null )
              {
                i.remove();
                continue;
              }
              cleanupNeededAt( cache.runCleanup( i ) );
            }
            
            
            cachesProcessed.addAll( cachesToProcess );
          }
        }
        finally
        {
          synchronized( MSCacheMap.class )
          {
            _cachesWithTimers = cachesProcessed;
            if( _cachesWithTimers.size() == 0 )
            {
              break; 
            }
          }
        }
        
        long sleepMillis = timeTillWakeup();
        
        
        
        Reference cacheRef = null;
        while( (cacheRef = _cachesClearedQueue.remove( sleepMillis )) != null )
        {
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          if(cacheRef instanceof WeakReference)
            break;
          
          sleepMillis = timeTillWakeup();
        }
      }
    }
    catch( InterruptedException x_ )
    {
    }
  }
  
  synchronized long runCleanup( Iterator cleanupListIter_ )
  {
    CacheInfo cache = _cache.info();
    if( cache == null )
    {
      
      
      
      
      
      
      return Long.MAX_VALUE;
    }
    long now = currentTimeMillis( this );
    final boolean unusedTimeout = _unusedTimeout != 0;
    final boolean staleTimeout = _staleTimeout != 0;
    LeastRecentList lrr = cache.lrrList();
    LeastRecentList lrw = cache.lrwList();
    CacheEntry leastRecentlyRead = null;
    CacheEntry leastRecentlyWritten = null;
    leastRecentlyRead = lrr.leastRecent();
    if( unusedTimeout )
    {
      while( leastRecentlyRead != null && (now - leastRecentlyRead.lastRead()) >= _unusedTimeout )
      {
        
        remove( leastRecentlyRead.getKey() );
        if( _listeners != null )
        {
          fireEntryUnusedTimeout( leastRecentlyRead );
        }
        leastRecentlyRead = lrr.leastRecent();
      }
    }
    leastRecentlyWritten = lrw.leastRecent();
    if( staleTimeout )
    {
      while( leastRecentlyWritten != null
          && (now - leastRecentlyWritten.lastWrite()) >= _staleTimeout )
      {
        
        remove( leastRecentlyWritten.getKey() );
        if( _listeners != null )
        {
          fireEntryStaleTimeout( leastRecentlyWritten );
        }
        leastRecentlyWritten = lrw.leastRecent();
      }
    }
    if( _usingTimeouts && size() == 0 && cleanupListIter_ != null )
    {
      cleanupListIter_.remove();
    }
    if( unusedTimeout && staleTimeout )
    {
      if( leastRecentlyRead != null && leastRecentlyWritten != null )
      {
        return Math.min( leastRecentlyRead.lastRead() + _unusedTimeout, leastRecentlyWritten
            .lastWrite()
            + _staleTimeout );
      }
      else if( leastRecentlyRead != null )
      {
        return leastRecentlyRead.lastRead() + _unusedTimeout;
      }
      else if( leastRecentlyWritten != null )
      {
        return leastRecentlyWritten.lastWrite() + _staleTimeout;
      }
    }
    else if( unusedTimeout )
    {
      if( leastRecentlyRead != null )
      {
        return leastRecentlyRead.lastRead() + _unusedTimeout;
      }
    }
    else if( staleTimeout )
    {
      if( leastRecentlyWritten != null )
      {
        return leastRecentlyWritten.lastWrite() + _staleTimeout;
      }
    }
    return Long.MAX_VALUE;
  }
  static final class SoftCacheInfo extends SoftReference
  {
    Object _hardRef;
    SoftCacheInfo( CacheInfo info_, boolean gc_ )
    {
      super( info_, _cachesClearedQueue );
      garbageCollectable( gc_ );
    }
    CacheInfo info()
    {
      return ( CacheInfo ) this.get();
    }
    HashMap map()
    {
      CacheInfo c = ( CacheInfo ) this.get();
      return (c == null) ? null : c.map();
    }
    Heap heap()
    {
      CacheInfo c = ( CacheInfo ) this.get();
      return (c == null) ? null : c.heap();
    }
    public void garbageCollectable( boolean gc_ )
    {
      if( !gc_ )
      {
        if( _hardRef == null )
          _hardRef = this.get();
      }
      else
      {
        _hardRef = null;
      }
    }
    public boolean garbageCollectable()
    {
      return _hardRef == null;
    }
  }
  static final class LeastRecentListNode
  {
    CacheEntry _entry;
    LeastRecentListNode _previous;
    LeastRecentListNode _next;
    LeastRecentListNode( CacheEntry entry_ )
    {
      _entry = entry_;
    }
    CacheEntry cacheEntry()
    {
      return _entry;
    }
    
    
    void unlink()
    {
      _previous._next = _next;
      _next._previous = _previous;
    }
    void insertAfter( LeastRecentListNode node_ )
    {
      _next = node_._next;
      _previous = node_;
      _previous._next = this;
      _next._previous = this;
    }
  }
  abstract static class LeastRecentList
  {
    LeastRecentListNode _header; 
    LeastRecentListNode _footer; 
    LeastRecentList()
    {
      _header = new LeastRecentListNode( null );
      _footer = new LeastRecentListNode( null );
      clear();
    }
    final void markMostRecent( CacheEntry entry_ )
    {
      LeastRecentListNode node = listNodeOf( entry_ );
      node.unlink();
      node.insertAfter(_header);
    }
    final CacheEntry leastRecent()
    {
      return _footer._previous.cacheEntry();
    }
    final void remove( CacheEntry entry_ )
    {
      listNodeOf( entry_ ).unlink();
      listNodeOf( entry_, null );
    }
    final void insert( CacheEntry entry_ )
    {
      LeastRecentListNode node = new LeastRecentListNode( entry_ );
      node.insertAfter( _header );
      listNodeOf( entry_, node );
    }
    final void clear()
    {
      _header._next = _footer;
      _footer._previous = _header;
    }
    abstract LeastRecentListNode listNodeOf( CacheEntry entry_ );
    abstract void listNodeOf( CacheEntry entry_, LeastRecentListNode node_ );
  }
  static final class LeastRecentReadList extends LeastRecentList
  {
    @Override
    LeastRecentListNode listNodeOf( CacheEntry entry_ )
    {
      return entry_._lrrNode;
    }
    @Override
    void listNodeOf( CacheEntry entry_, LeastRecentListNode node_ )
    {
      entry_._lrrNode = node_;
    }
  }
  static final class LeastRecentWriteList extends LeastRecentList
  {
    @Override
    LeastRecentListNode listNodeOf( CacheEntry entry_ )
    {
      return entry_._lrwNode;
    }
    @Override
    void listNodeOf( CacheEntry entry_, LeastRecentListNode node_ )
    {
      entry_._lrwNode = node_;
    }
  }
  static final class CacheInfo
  {
    
    final Heap _heap;
    final LeastRecentList _lrrList; 
    final LeastRecentList _lrwList; 
    final HashMap _map;
    CacheInfo( HashMap map_, Heap heap_ )
    {
      _heap = heap_;
      _lrrList = new LeastRecentReadList();
      _lrwList = new LeastRecentWriteList();
      _map = map_;
    }
    Heap heap()
    {
      return _heap;
    }
    HashMap map()
    {
      return _map;
    }
    LeastRecentList lrrList()
    {
      return _lrrList;
    }
    LeastRecentList lrwList()
    {
      return _lrwList;
    }
  }
  
  static class Heap
  {
    protected CacheEntry[] _nodes; 
    protected int count_ = 0; 
    protected final Comparator _cmp; 
    
    Heap( int capacity, Comparator cmp ) throws IllegalArgumentException
    {
      if( capacity <= 0 )
        throw new IllegalArgumentException();
      _nodes = new CacheEntry[capacity];
      _cmp = cmp;
    }
    
    
    
    
    
    
    protected int compare( CacheEntry a, CacheEntry b )
    {
      if( _cmp == null )
        return (( Comparable ) a).compareTo( b );
      else
        return _cmp.compare( a, b );
    }
    
    protected final int parent( int k )
    {
      return (k - 1) >> 1;
    }
    protected final int left( int k )
    {
      return (k << 1) + 1;
    }
    protected final int right( int k )
    {
      return (k + 1) << 1;
    }
    protected final void moveTo( int to_, CacheEntry entry_ )
    {
      entry_._heapLocation = to_;
      _nodes[to_] = entry_;
    }
    
    
    
    
    void moveUpwards( int k_, CacheEntry x_ )
    {
      while( k_ > 0 )
      {
        int par = parent( k_ );
        if( compare( x_, _nodes[par] ) < 0 )
        {
          moveTo( k_, _nodes[par] );
          k_ = par;
        }
        else
          break;
      }
      moveTo( k_, x_ );
    }
    void moveDownwards( int k_, CacheEntry x_ )
    {
      for( ;; )
      {
        int l = left( k_ );
        if( l >= count_ )
          break;
        else
        {
          int r = right( k_ );
          int child = (r >= count_ || compare( _nodes[l], _nodes[r] ) < 0) ? l : r;
          if( compare( x_, _nodes[child] ) > 0 )
          {
            moveTo( k_, _nodes[child] );
            k_ = child;
          }
          else
            break;
        }
      }
      moveTo( k_, x_ );
    }
    
    void insert( CacheEntry x )
    {
      if( count_ >= _nodes.length )
      {
        int newcap = 3 * _nodes.length / 2 + 1;
        CacheEntry[] newnodes = new CacheEntry[newcap];
        System.arraycopy( _nodes, 0, newnodes, 0, _nodes.length );
        _nodes = newnodes;
      }
      int k = count_;
      ++count_;
      moveUpwards( k, x );
    }
    
    Object extract()
    {
      if( count_ < 1 )
        return null;
      int k = 0; 
      CacheEntry least = _nodes[k];
      --count_;
      CacheEntry x = _nodes[count_];
      moveDownwards( k, x );
      
      _nodes[count_] = null;
      return least;
    }
    void remove( CacheEntry x_ )
    {
      --count_;
      int offset = x_._heapLocation;
      moveTo( offset, _nodes[count_] );
      moveDownwards( offset, _nodes[count_] );
      
      _nodes[count_] = null;
    }
    
    void heapify( CacheEntry entryChanged_ )
    {
      int offset = entryChanged_._heapLocation;
      CacheEntry x = _nodes[offset];
      if( offset > 0 )
      {
        int par = parent( offset );
        int cmp = compare( x, _nodes[par] );
        if( cmp < 0 ) 
        {
          moveUpwards( offset, x );
        }
        else if( cmp > 0 )
        {
          moveDownwards( offset, x );
        }
      }
      else
      {
        moveDownwards( offset, x );
      }
    }
    
    CacheEntry peek()
    {
      if( count_ > 0 )
        return _nodes[0];
      else
        return null;
    }
    
    int size()
    {
      return count_;
    }
    
    void clear()
    {
      count_ = 0;
      for( int i = 0, n = _nodes.length; i < n; ++i )
      {
        _nodes[i] = null;
      }
    }
  }
}