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

package msjava.base.xml;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
public class SchemaLocations
{
  private Map _map = new LinkedHashMap();
  private String _noNamespaceSchemaLocation;
  public SchemaLocations()
  {
  }
  public SchemaLocations( String noNamespaceSchemaLocation_ )
  {
    _noNamespaceSchemaLocation = noNamespaceSchemaLocation_;
  }
  public SchemaLocations( Map map_ )
  {
    this( map_, null );
  }
  public SchemaLocations( Map map_, String noNamespaceSchemaLocation_ )
  {
    _map.putAll( map_ );
    _noNamespaceSchemaLocation = noNamespaceSchemaLocation_;
  }
  
  public void setNoNamespaceSchemaLocation( String noNamespaceSchemaLocation_ )
  {
    _noNamespaceSchemaLocation = noNamespaceSchemaLocation_;
  }
  
  public String getNoNamespaceSchemaLocation()
  {
    return _noNamespaceSchemaLocation;
  }
  
  public SchemaLocations addLocation( String namespace_, String location_ )
  {
    _map.put( namespace_, location_ );
    return this;
  }
  
  public SchemaLocations addLocations( Map schemaLocations_ )
  {
    _map.putAll( schemaLocations_ );
    return this;
  }
  
  public String getLocation( String namespace_ )
  {
    return ( String ) _map.get( namespace_ );
  }
  
  public String getSchemaLocationsPropertyValue()
  {
    return SchemaLocationsUtils.toXercesSchemaLocationsProperty( this );
  }
  
  public Map getSchemaMappings()
  {
    return Collections.unmodifiableMap( _map );
  }
  public String toString()
  {
    StringBuffer buffer = new StringBuffer( super.toString() );
    buffer.append( "SchemaLocation Mappings:" );
    if( _noNamespaceSchemaLocation != null )
    {
      buffer.append( "No Namespace => " ).append( _noNamespaceSchemaLocation ).append( "\n" );
    }
    for( Iterator i = _map.entrySet().iterator(); i.hasNext(); )
    {
      Entry e = ( Entry ) i.next();
      buffer.append( "'" ).append( e.getKey() ).append( "' => " ).append( e.getValue() ).append(
          "\n" );
    }
    return buffer.toString();
  }
  public int hashCode()
  {
    int code = _noNamespaceSchemaLocation == null ? 0 : _noNamespaceSchemaLocation.hashCode();
    return code + _map.hashCode();
  }
  public boolean equals( Object obj_ )
  {
    if( !(obj_ instanceof SchemaLocations) )
    {
      return false;
    }
    SchemaLocations that = ( SchemaLocations ) obj_;
    if( that._noNamespaceSchemaLocation != _noNamespaceSchemaLocation )
    {
      return false;
    }
    if( !_map.equals( that._map ) )
    {
      return false;
    }
    return true;
  }
}