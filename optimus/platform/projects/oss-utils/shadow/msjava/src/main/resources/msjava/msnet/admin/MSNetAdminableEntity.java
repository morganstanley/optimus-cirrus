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

package msjava.msnet.admin;
import java.util.Date;
import msjava.msnet.MSNetAddress;
import msjava.msnet.MSNetID;
import msjava.msnet.MSNetState;
public abstract interface MSNetAdminableEntity {
    
    public MSNetAddress getAddress();
    
    public void close();
    
    public String getEntityType();
    
    public String getInfoMessage();
    
    public MSNetState getState();
    
    public boolean isOpen();
    
    public boolean isValid();
    
    public MSNetID getName();
    
    public void open();
    
    public void open(MSNetAddress address_);
    
    public Date openTime();
    
    public void addListener(MSNetAdminableEntityListener listener_);
    
    public void removeListener(MSNetAdminableEntityListener listener_);
    
    public void setProperty(String name_, Object value_);
    
    public Object getProperty(String name_);
}
