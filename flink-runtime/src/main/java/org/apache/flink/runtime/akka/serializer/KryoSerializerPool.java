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

package org.apache.flink.runtime.akka.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.objenesis.strategy.StdInstantiatorStrategy;

/**
 * Pool for instance of {@link Kryo} {@link Input} and {@link Output}. Each thread will
 * have its thread local instance.
 */
public class KryoSerializerPool {

	private static final ThreadLocal<Kryo> kryos = ThreadLocal.withInitial(() -> {
		Kryo kryo = new Kryo();
		kryo.setRegistrationRequired(false);
		Kryo.DefaultInstantiatorStrategy instStrategy = new Kryo.DefaultInstantiatorStrategy();
		instStrategy.setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
		kryo.setInstantiatorStrategy(instStrategy);
		return kryo;
	});

	private static final ThreadLocal<Output> outputs = ThreadLocal.withInitial(() -> new Output(1024, -1));

	private static final ThreadLocal<Input> inputs = ThreadLocal.withInitial(Input::new);

	public Kryo getKryo() {
		return kryos.get();
	}

	public Output getOutput() {
		return outputs.get();
	}

	public Input getInput() {
		return inputs.get();
	}
}
