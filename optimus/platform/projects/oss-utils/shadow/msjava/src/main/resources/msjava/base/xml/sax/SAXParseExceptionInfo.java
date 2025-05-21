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

package msjava.base.xml.sax;
import org.xml.sax.SAXParseException;
public class SAXParseExceptionInfo
{
  protected final SAXParseException _exception;
  protected final String _elementQName;
  protected String _location;
  public SAXParseExceptionInfo( SAXParseException exception_, String elementQName_ )
  {
    _exception = exception_;
    _elementQName = elementQName_;
  }
  public String getElementQName()
  {
    return _elementQName;
  }
  
  public String getLocation()
  {
    if( _location == null )
    {
      _location = toLocationDescription( _exception );
    }
    return _location;
  }
  public SAXParseException getSAXParseException()
  {
    return _exception;
  }
  
  public static String toLocationDescription( SAXParseException exception_ )
  {
    StringBuffer buffer = new StringBuffer();
    String systemId = exception_.getSystemId();
    if( systemId != null )
    {
      int index = systemId.lastIndexOf( '/' );
      if( index != -1 )
      {
        systemId = systemId.substring( index + 1 );
      }
      buffer.append( systemId );
      buffer.append( ':' );
    }
    buffer.append( exception_.getLineNumber() );
    buffer.append( ':' );
    buffer.append( exception_.getColumnNumber() );
    return buffer.toString();
  }
}