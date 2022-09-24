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

package org.apache.flink.runtime.io.network.api.reader;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Implementation of the record-oriented reader for mutable record types.
 *
 * @param <T> The type of the record that is read.
 */
public class MutableRecordReader<T extends IOReadableWritable> extends AbstractRecordReader<T> implements MutableReader<T> {

	/**
	 * Creates a new MutableRecordReader that de-serializes records from the given input gate and
	 * can spill partial records to disk, if they grow large.
	 *
	 * @param inputGate The input gate to read from.
	 * @param tmpDirectories The temp directories. USed for spilling if the reader concurrently
	 *                       reconstructs multiple large records.
	 */
	public MutableRecordReader(InputGate inputGate, String[] tmpDirectories) {
		super(inputGate, tmpDirectories);
	}

	@Override
	public boolean next(final T target) throws IOException, InterruptedException {
		return getNextRecord(target);
	}

	public byte[] getBytes () {
		ArrayList<byte[]> totalBytes = new ArrayList<>();
		try {
			while (getSpillingAdaptiveBytes(totalBytes)) ;
		} catch (Exception e) {
			System.out.println("Exception during spillingAdaptive call");
			System.out.println(e);
		}
		int size = 0;
		//System.out.println("--- totalBytes.size: " + totalBytes.size());
		for (byte[] b : totalBytes) {
			size += b.length;
		}
			/*if (size == 0) {
			    System.out.println("--- size = 0, return");
			    return null;
			}*/
		byte[] total = null;

		int destPos = 0;

		for (byte[] b : totalBytes) {
			if (total == null) {
				// write first array
				total = Arrays.copyOf(b, size);
				destPos += b.length;
			} else {
				System.arraycopy(b, 0, total, destPos, b.length);
				destPos += b.length;
			}
		}
		//System.out.println("--- total.length: " + total.length);
		return total;
		//return getSpillingAdaptiveBytes();
	}

	@Override
	public void clearBuffers() {
		super.clearBuffers();
	}
}
