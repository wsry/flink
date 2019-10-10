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

package org.apache.flink.runtime.rpc.serializer;

import java.io.IOException;

/**
 * A serializer wrapper which chooses the corresponding serializer according to configuration.
 */
public class RpcSerializationUtil {

	public static RpcSerializerType serializerType = RpcSerializerType.FAST_SERIALIZATION;

	public static byte[] serialize(Object object) throws IOException {
		switch (serializerType) {
			case JAVA_INTERNAL:
				return DefaultRpcMessageSerializer.serialize(object);
			case KRYO:
				return KryoBasedRpcMessageSerializer.serialize(object);
			case FAST_SERIALIZATION:
				return FSTBasedRpcMessageSerializer.serialize(object);
			default:
				throw new RuntimeException("Unknown rpc message serializer type.");
		}
	}

	public static <T> T deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
		switch (serializerType) {
			case JAVA_INTERNAL:
				return DefaultRpcMessageSerializer.deserialize(bytes);
			case KRYO:
				return KryoBasedRpcMessageSerializer.deserialize(bytes);
			case FAST_SERIALIZATION:
				return FSTBasedRpcMessageSerializer.deserialize(bytes);
			default:
				throw new RuntimeException("Unknown rpc message serializer type.");
		}
	}

	public enum RpcSerializerType {
		JAVA_INTERNAL,
		KRYO,
		FAST_SERIALIZATION
	}
}
