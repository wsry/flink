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

import org.apache.flink.runtime.util.Hardware;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * A kryo-based serializer for flink rpc message serialization.
 */
public class KryoBasedRpcMessageSerializer {

	private static final KryoSerializerPool serializerPool = new KryoSerializerPool();

	public static byte[] serialize(Object object) throws IOException {
		KryoSerializer serializer = null;
		try {
			serializer = serializerPool.take();
			return serializer.serialize(object);
		} catch (InterruptedException e) {
			throw new IOException("Interrupted.", e);
		} finally {
			if (serializer != null) {
				serializerPool.recycle(serializer);
			}
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> T deserialize(byte[] bytes) throws IOException {
		KryoSerializer serializer = null;
		try {
			serializer = serializerPool.take();
			return (T) serializer.deserialize(bytes);
		} catch (InterruptedException e) {
			throw new IOException("Interrupted.", e);
		} finally {
			if (serializer != null) {
				serializerPool.recycle(serializer);
			}
		}
	}

	/**
	 * A pool of {@link KryoSerializer}.
	 */
	private static class KryoSerializerPool {

		private final int POOL_SIZE = 2 * Hardware.getNumberCPUCores();

		private final BlockingQueue<KryoSerializer> pool = new ArrayBlockingQueue<>(POOL_SIZE);

		private KryoSerializerPool() {
			for (int i = 0; i < POOL_SIZE; ++i) {
				pool.add(new KryoSerializer());
			}
		}

		private KryoSerializer take() throws InterruptedException {
			return pool.take();
		}

		private void recycle(KryoSerializer serializer) {
			pool.add(serializer);
		}
	}

	/**
	 * The kryo serializer backend.
	 */
	private static class KryoSerializer {

		private final Kryo kryo;

		private final Input input;

		private final Output output;

		private KryoSerializer() {
			kryo = initKryo(new Kryo());
			input = new Input();
			output = new Output(4096, -1);
		}

		private byte[] serialize(Object object) {
			output.clear();
			kryo.writeClassAndObject(output, object);
			return output.toBytes();
		}

		public Object deserialize(byte[] bytes) {
			input.setBuffer(bytes);
			return kryo.readClassAndObject(input);
		}

		private Kryo initKryo(Kryo kryo) {
			JavaSerializer fallBack = new JavaSerializer();
			kryo.setRegistrationRequired(false);
			kryo.register(InetSocketAddress.class, fallBack);
			kryo.register(InetAddress.class, fallBack);
			kryo.register(Inet4Address.class, fallBack);
			kryo.register(Inet6Address.class, fallBack);
			kryo.register(Arrays.asList("").getClass(), fallBack);
			kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
			Kryo.DefaultInstantiatorStrategy instStrategy = new Kryo.DefaultInstantiatorStrategy();
			instStrategy.setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
			kryo.setInstantiatorStrategy(instStrategy);
			return kryo;
		}
	}
}
