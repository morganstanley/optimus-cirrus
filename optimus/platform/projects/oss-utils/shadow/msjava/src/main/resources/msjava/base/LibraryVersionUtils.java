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
package msjava.base;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.CodeSource;
import java.util.Map.Entry;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.jar.Attributes.Name;
import msjava.base.io.IOUtils;
public class LibraryVersionUtils {
    public static final String UNKNOWN_VERSION = "?";
    
    public static String getPatchVersionOrDefault(Class<?> klass) {
        return defaultIfNull(getPatchVersion(klass), UNKNOWN_VERSION);
    }
    
    public static String getVersionOrDefault(Class<?> klass) {
        return defaultIfNull(getVersion(klass), UNKNOWN_VERSION);
    }
    private static String defaultIfNull(String value, String default_) {
        return value == null ? default_ : value;
    }
    
    public static String getPatchVersion(Class<?> klass) {
        return findAttribute(Name.IMPLEMENTATION_VERSION, klass);
    }
    public static String getVersion(Class<?> klass) {
        return findAttribute(Name.SPECIFICATION_VERSION, klass);
    }
    
    private static String findAttribute(Name searchingFor, Class<?> clazz) {
        CodeSource codeSource = clazz.getProtectionDomain().getCodeSource();
        if (codeSource == null || codeSource.getLocation() == null) {
            return null;
        }
        String codeLocation = codeSource.getLocation().toString();
        InputStream manifestStream = null;
        try {
            URL manifestUrl = new URL("jar:" + codeLocation + "!/META-INF/MANIFEST.MF");
            manifestStream = manifestUrl.openStream();
            Manifest manifest = new Manifest(manifestStream);
            for (Entry<String, Attributes> entry : manifest.getEntries().entrySet()) {
                Object value = entry.getValue().getValue(searchingFor);
                if (value != null) {
                    return (String) value;
                }
            }
            Object value = manifest.getMainAttributes().getValue(searchingFor);
            if (value != null) {
                return (String) value;
            }
        } catch (IOException e) {
            
        } finally {
            IOUtils.closeQuietly(manifestStream);
        }
        return null;
    }
}
