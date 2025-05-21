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
public class MSAdminCommandStoreItemBaseImpl implements MSAdminCommandStoreItem {
    protected String _namespace;
    protected String _name;
    
    protected MSAdminCommandStoreItemBaseImpl() {
        
    }
    
    public MSAdminCommandStoreItemBaseImpl(String namespace_, String name_) throws IllegalArgumentException {
        checkNamespace(namespace_);
        checkName(name_);
        _namespace = namespace_;
        _name = name_;
    }
    
    public String getName() {
        return _name;
    }
    
    public String getNamespace() {
        return _namespace;
    }
    
    protected static void checkNamespace(String namespace_) throws IllegalArgumentException {
        if (!MSAdminUtil.isValidNamespace(namespace_)) {
            throw new IllegalArgumentException("\"" + namespace_
                    + "\" is not a valid namespace - should be of the form \"foo.bar.baz\"");
        }
    }
    
    protected static void checkName(String name_) throws IllegalArgumentException {
        if (!MSAdminUtil.isValidName(name_)) {
            throw new IllegalArgumentException("\"" + name_
                    + "\" is not a valid name - should be of the form \"foo_bar\"");
        }
    }
    
    @Override
    public boolean equals(Object o_) {
        if (o_ instanceof MSAdminCommandStoreItem) {
            MSAdminCommandStoreItem item = (MSAdminCommandStoreItem) o_;
            return item.getName().equals(getName()) && item.getNamespace().equals(getNamespace());
        }
        return false;
    }
    
    public int hashCode() {
        return getName().hashCode() / 2 + getNamespace().hashCode() / 2;
    }
    
    public String toString() {
        return getClass().getName() + ": " + getNamespace() + "." + getName();
    }
    
    public int compareTo(Object o_) {
        MSAdminCommandStoreItem item = (MSAdminCommandStoreItem) o_;
        int namespaceComparison = getNamespace().compareTo(item.getNamespace());
        if (namespaceComparison == 0) {
            
            return getName().compareTo(item.getName());
        }
        return namespaceComparison;
    }
}
