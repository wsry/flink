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

import akka.serialization.JSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * An alternative kryo-based serializer for Flink rpc to be used by akka.
 */
public class AkkaKryoSerializer extends JSerializer {

	private KryoSerializerPool serializerPool = new KryoSerializerPool();

	@Override
	public int identifier() {
		// a unique identifier for this serializer
		return 186523407;
	}

	@Override
	public boolean includeManifest() {
		return false;
	}

	@Override
	public Object fromBinaryJava(byte[] bytes, Class<?> manifest) {
		Kryo kryo = serializerPool.getKryo();
		Input input = serializerPool.getInput();
		input.setBuffer(bytes);
		return kryo.readClassAndObject(input);
	}

	@Override
	public byte[] toBinary(Object object) {
		Kryo kryo = serializerPool.getKryo();
		Output output = serializerPool.getOutput();
		output.clear();
		kryo.writeClassAndObject(output, object);
		return output.toBytes();
	}
}
