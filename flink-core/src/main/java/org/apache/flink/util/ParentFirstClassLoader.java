/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import java.net.URL;

/**
 * URLClassLoader that first loads from the parent, then from the URLs and finally from the extra ClassLoaders.
 */
public class ParentFirstClassLoader extends FlinkUserCodeClassLoader {

	public ParentFirstClassLoader(URL[] urls, ClassLoader parent) {
		this(null, urls, parent);
	}

	public ParentFirstClassLoader(FlinkUserCodeClassLoader[] extClassLoaders, URL[] urls, ClassLoader parent) {
		super(extClassLoaders, urls, parent);
	}

	@Override
	protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
		try {
			return super.loadClass(name, resolve);
		} catch (ClassNotFoundException e) {
			Class<?> c = tryLoadClassFromExtraClassLoaders(name);
			if (c == null) {
				throw e;
			}
			if (resolve) {
				resolveClass(c);
			}
			return c;
		}
	}
}
