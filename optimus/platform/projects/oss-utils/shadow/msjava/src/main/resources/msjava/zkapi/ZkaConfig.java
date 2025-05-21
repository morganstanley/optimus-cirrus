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

package msjava.zkapi;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.Map.Entry;
import msjava.base.slf4j.ContextLogger;
import msjava.base.slr.internal.ServiceEnvironment;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.slf4j.Logger;
import org.springframework.util.StringUtils;
public final class ZkaConfig {
	
	private static final Logger logger = ContextLogger.safeLogger();
	private static final Map<String, ZkaAttr> ATTRIBUTES;
	private ServiceEnvironment environment;
	private String region;
	private String basePath;
	private String ensemble; 
	private boolean hosted = true; 
	private final Map<ZkaAttr, String> attributes = new EnumMap<>(ZkaAttr.class);
	private static final String INVALID_PATH = "path must not be empty";
	private static final String INVALID_AUTHORITY = "authority (ensemble) must not be empty";
	private static final String MISSING_SCHEME = "scheme must be present, such as zps or zle";
	
	static {
		Builder<String, ZkaAttr> builder = ImmutableMap.builder();
		for (final ZkaAttr za : ZkaAttr.values()) {
			builder.put(za.getKey(), za);
		}
		ATTRIBUTES = builder.build();
	}
	public static ZkaConfig fromURI(String uriString) throws URISyntaxException {
		final URI uri = new URI(uriString);
		return fromURI(uri);
	}
	
	public static ZkaConfig fromURI(URI uri) throws IllegalArgumentException {
		String ensemble = uri.getAuthority();
		validateURI(uri);
		final ZkaConfig zkc = new ZkaConfig();
		zkc.setPath(uri.getPath());
		logger.debug("basePath {}", zkc.basePath);
		zkc.setAttributes(URLEncodedUtils.parse(uri, StandardCharsets.UTF_8));
		zkc.setEnsemble(ensemble);
		if (zkc.isHosted()) {
			final String[] tokens = uri.getHost().split("\\.");
			zkc.setRegion(tokens[1]);
			zkc.setEnvironment(resolveServiceEnvironment(tokens[0]));
		}
		return zkc;
	}
	private static void validateURI(URI uri) {
		Preconditions.checkArgument(!StringUtils.isEmpty(uri.getScheme()), MISSING_SCHEME);
		Preconditions.checkArgument(!StringUtils.isEmpty(uri.getPath()), INVALID_PATH);
		Preconditions.checkArgument(!StringUtils.isEmpty(uri.getAuthority()), INVALID_AUTHORITY);
	}
	private static ServiceEnvironment resolveServiceEnvironment(String env) {
		try {
			return ServiceEnvironment.valueOf(env);
		} catch (IllegalArgumentException e) {
	           throw new IllegalArgumentException(env + " is not a resolvable environment. Supported environments: " + Arrays.toString(ServiceEnvironment.values()), e);
		}
	}
	private static boolean checkEnsembleHosted(String ensemble) {
		if (ensemble == null)
			return true; 
							
		int pos = ensemble.indexOf('.');
		
		return (pos != -1) 
				&& (pos == ensemble.lastIndexOf('.')); 
														
														
	}
	public ZkaConfig ensemble(String ensemble) {
		this.setEnsemble(ensemble);
		return this;
	}
	public ZkaConfig environment(ServiceEnvironment environment) {
		this.setEnvironment(environment);
		return this;
	}
	public ZkaConfig region(String region) {
		this.setRegion(region);
		return this;
	}
	 
	@Deprecated
	public ZkaConfig basePath(String path) {
		this.setPath(path);
		return this;
	}
	public ZkaConfig path(String path) {
	    this.setPath(path);
	    return this;
	}
	
	public ZkaConfig attr(ZkaAttr attr, String value, boolean overwrite) {
		if (overwrite) {
			attributes.put(attr, value);
		} else {
			attributes.putIfAbsent(attr, value);
		}
		return this;
	}
	
	public ZkaConfig attr(ZkaAttr attr, int value, boolean overwrite) {
		Preconditions.checkArgument(attr.getType() == int.class, attr.getKey() + " expects a value of type " + attr.getType());
		if (overwrite) {
			attributes.put(attr, "" + value);
		} else {
			attributes.putIfAbsent(attr, "" + value);
		}
		return this;
	}
	
	public ZkaConfig attr(ZkaAttr attr, boolean value, boolean overwrite) {
		Preconditions.checkArgument(attr.getType() == boolean.class, attr.getKey() + " expects a value of type " + attr.getType());
		if (overwrite) {
			attributes.put(attr, "" + value);
		} else {
			attributes.putIfAbsent(attr, "" + value);
		}
		return this;
	}
	
	public ZkaConfig attr(ZkaAttr attr, String value) {
		return attr(attr, value, true);
	}
	
	public ZkaConfig attr(ZkaAttr attr, int value) {
		return attr(attr, value, true);
	}
	
	public ZkaConfig attr(ZkaAttr attr, boolean value) {
		return attr(attr, value, true);
	}
	private void setAttributes(Iterable<NameValuePair> attirbutes) {
		for (NameValuePair nvp : attirbutes) {
			setAttribute(nvp.getName(), nvp.getValue());
		}
	}
	public void setAttributes(Map<String, String> attributes) {
		for (final Entry<String, String> attr : attributes.entrySet()) {
			setAttribute(attr.getKey(), attr.getValue());
		}
	}
	public void setAttribute(String key, String value) {
		final ZkaAttr za = ATTRIBUTES.get(key);
		if (za == null)
			logger.warn("The attribute {} is not used.", key);
		else
			this.attributes.put(za, value);
	}
	public ServiceEnvironment getEnvironment() {
		return environment;
	}
	public void setEnvironment(ServiceEnvironment environment) {
		this.environment = environment;
		this.ensemble = environment.toString() + "." + region;
		setHosted(true);
	}
	
	@Deprecated
	public String getBasePath() {
		return basePath;
	}
	
	@Deprecated
	public void setBasePath(String basePath) {
		this.basePath = basePath;
	}
	public String getPath() {
	    return basePath;
	}
	
	public void setPath(String basePath) {
	    this.basePath = basePath;
	}
	public String getEnsemble() {
		return ensemble;
	}
	
	public void setEnsemble(String ensemble) {
		boolean hosted = checkEnsembleHosted(ensemble);
		setHosted(hosted);
		if (isHosted()) {
			if (ensemble.split("\\.").length == 2) {
				String[] parts = ensemble.split("\\.", 2);
				setEnvironment(resolveServiceEnvironment(parts[0]));
				setRegion(parts[1]);
			} else {
				throw new RuntimeException("Host must be of the format '${ENV}.${REGION}'");
			}
		} else {
			boolean krb = getBoolean(ZkaAttr.KERBEROS);
			if (ensemble.contains("@") && !krb) {
				logger.warn("Ensemble specification contains proid but kerberize=false. This is not the usual usage and proid will be ignored.");
			}
		}
		this.ensemble = ensemble;
	}
	
	public boolean isHosted() {
		return hosted;
	}
	
	private void setHosted(boolean hosted) {
		this.hosted = hosted;
	}
	public String getRegion() {
		return region;
	}
	
	public void setRegion(String region) {
		this.region = region;
		this.ensemble = environment + "." + region;
		setHosted(true);
	}
	public Map<ZkaAttr, String> getAttributes() {
		return attributes;
	}
	
	public boolean getBoolean(ZkaAttr attr) {
		assert attr.getType().equals(Boolean.class) || attr.getType().equals(boolean.class);
        String v = attributes.get(attr);
        return v == null ? (Boolean) attr.getDefaultValue() : Boolean.parseBoolean(v);
	}
	public int getInteger(ZkaAttr attr) {
	    assert attr.getType().equals(Integer.class) || attr.getType().equals(int.class);
	    String v = attributes.get(attr);
	    return v == null ? (Integer) attr.getDefaultValue() : Integer.parseInt(v);
	}
	public ZkaConfig copy() {
		ZkaConfig c = new ZkaConfig();
		c.basePath = basePath;
		c.environment = environment;
		c.region = region;
		c.ensemble = ensemble;
		c.hosted = hosted;
		c.attributes.putAll(attributes);
		return c;
	}
}
