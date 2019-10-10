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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.objenesis.strategy.StdInstantiatorStrategy;

/**
 * A kryo-based serializer for flink rpc message serialization.
 */
public class KryoBasedRpcMessageSerializer {

	public static byte[] serialize(Object object) {
		Kryo kryo = KryoSerializerPool.getKryo();
		Output output = KryoSerializerPool.getOutput();
		output.clear();
		kryo.writeClassAndObject(output, object);
		return output.toBytes();
	}

	@SuppressWarnings("unchecked")
	public static <T> T deserialize(byte[] bytes) {
		Kryo kryo = KryoSerializerPool.getKryo();
		Input input = KryoSerializerPool.getInput();
		input.setBuffer(bytes);
		return (T) kryo.readClassAndObject(input);
	}

	private static class KryoSerializerPool {

		private static final ThreadLocal<Kryo> kryos = ThreadLocal.withInitial(() -> {
			Kryo kryo = new Kryo();
			kryo.setRegistrationRequired(false);
			Kryo.DefaultInstantiatorStrategy instStrategy = new Kryo.DefaultInstantiatorStrategy();
			instStrategy.setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
			kryo.setInstantiatorStrategy(instStrategy);
			return kryo;
		});

		private static final ThreadLocal<Input> inputs = ThreadLocal.withInitial(Input::new);

		private static final ThreadLocal<Output> outputs = ThreadLocal.withInitial(() -> new Output(1024, -1));

		public static Kryo getKryo() {
			return kryos.get();
		}

		public static Input getInput() {
			return inputs.get();
		}

		public static Output getOutput() {
			return outputs.get();
		}
	}
}
