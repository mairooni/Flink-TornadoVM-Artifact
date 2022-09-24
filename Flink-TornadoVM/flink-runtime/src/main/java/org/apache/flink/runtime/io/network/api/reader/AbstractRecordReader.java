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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A record-oriented reader.
 *
 * <p>This abstract base class is used by both the mutable and immutable record readers.
 *
 * @param <T> The type of the record that can be read with this record reader.
 */
abstract class AbstractRecordReader<T extends IOReadableWritable> extends AbstractReader implements ReaderBase {

	private final Map<InputChannelInfo, RecordDeserializer<T>> recordDeserializers;

	private RecordDeserializer<T> currentRecordDeserializer;

	private boolean requestedPartitions;

	private boolean isFinished;

	private boolean endOfSuperStep;

	/**
	 * Creates a new AbstractRecordReader that de-serializes records from the given input gate and
	 * can spill partial records to disk, if they grow large.
	 *
	 * @param inputGate The input gate to read from.
	 * @param tmpDirectories The temp directories. USed for spilling if the reader concurrently
	 *                       reconstructs multiple large records.
	 */
	@SuppressWarnings("unchecked")
	protected AbstractRecordReader(InputGate inputGate, String[] tmpDirectories) {
		super(inputGate);

		// Initialize one deserializer per input channel
		recordDeserializers = inputGate.getChannelInfos().stream()
			.collect(Collectors.toMap(
				Function.identity(),
				channelInfo -> new SpillingAdaptiveSpanningRecordDeserializer<>(tmpDirectories)));
	}

//	public byte[] getSpillingAdaptiveBytes() {
//		ArrayList<byte[]> totalBytes = new ArrayList<>();
//		try {
//			boolean flag = false;
//			while (true) {
//				if (!requestedPartitions) {
//					inputGate.requestPartitions();
//					requestedPartitions = true;
//				}
//
//				if (isFinished) {
//					break;
//				}
//
//				while (true) {
//					if (currentRecordDeserializer != null) {
//						if (currentRecordDeserializer instanceof SpillingAdaptiveSpanningRecordDeserializer) {
//							SpillingAdaptiveSpanningRecordDeserializer spAdapt = (SpillingAdaptiveSpanningRecordDeserializer) currentRecordDeserializer;
//							Tuple2<DeserializationResult, byte[]> tup = spAdapt.getNonSpanningBytes();
//							totalBytes.add(tup.f1);
//
//							if (tup.f0.isBufferConsumed()) {
//								final Buffer currentBuffer = currentRecordDeserializer.getCurrentBuffer();
//
//								currentBuffer.recycleBuffer();
//								currentRecordDeserializer = null;
//							}
//
//							if (tup.f0.isFullRecord()) {
//								flag = true;
//								break;
//							}
//						}
//					}
//
//					final BufferOrEvent bufferOrEvent = inputGate.getNext().orElseThrow(IllegalStateException::new);
//
//					if (bufferOrEvent.isBuffer()) {
//						//System.out.println(">>>>> isBuffer!");
//						currentRecordDeserializer = recordDeserializers.get(bufferOrEvent.getChannelInfo());
//						currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
//					}
//					else {
//						//System.out.println(">>>>> isEvent!");
//						// sanity check for leftover data in deserializers. events should only come between
//						// records, not in the middle of a fragment
//						if (recordDeserializers.get(bufferOrEvent.getChannelInfo()).hasUnfinishedData()) {
//							throw new IOException(
//								"Received an event in channel " + bufferOrEvent.getChannelInfo() + " while still having "
//									+ "data from a record. This indicates broken serialization logic. "
//									+ "If you are using custom serialization code (Writable or Value types), check their "
//									+ "serialization routines. In the case of Kryo, check the respective Kryo serializer.");
//						}
//
//						if (handleEvent(bufferOrEvent.getEvent())) {
//							//System.out.println(">>>>> handledEvent!");
//							if (inputGate.isFinished()) {
//								//System.out.println(">>>>> inputGate finished!");
//								isFinished = true;
//								flag = true;
//								break;
//							}
//							else if (hasReachedEndOfSuperstep()) {
//								//System.out.println(">>>>> End of superstep!");
//								flag = true;
//								break;
//							}
//							// else: More data is coming...
//						}
//					}
//					if (flag) break;
//				}
//			}
//			int size = 0;
//			for (byte[] b : totalBytes) {
//				size += b.length;
//			}
//			byte[] total = null;
//
//			int destPos = 0;
//
//			for (byte[] b : totalBytes) {
//				if (total == null) {
//					// write first array
//					total = Arrays.copyOf(b, size);
//					destPos += b.length;
//				} else {
//					System.arraycopy(b, 0, total, destPos, b.length);
//					destPos += b.length;
//				}
//			}
//			return total;
//		} catch (Exception e) {
//			System.out.println("Exception in AbstractRecordReader");
//			return null;
//		}
//	}

	public boolean getSpillingAdaptiveBytes(ArrayList<byte[]> totalBytes) throws IOException, InterruptedException {
		// The action of partition request was removed from InputGate#setup since FLINK-16536, and this is the only
		// unified way for launching partition request for batch jobs. In order to avoid potential performance concern,
		// we might consider migrating this action back to the setup based on some condition judgement future.
		if (!requestedPartitions) {
			inputGate.requestPartitions();
			requestedPartitions = true;
			//System.out.println(">>>>> Partitions requested!");
		}

		if (isFinished) {
			//System.out.println(">>>>> Finished, return false!");
			return false;
		}

		while (true) {
			if (currentRecordDeserializer != null) {
				//System.out.println(">>>>> currentRecordDeserializer is not null!");
				if (currentRecordDeserializer instanceof SpillingAdaptiveSpanningRecordDeserializer) {
					//System.out.println("--- record deserializer spillingadaptive");
					SpillingAdaptiveSpanningRecordDeserializer spAdapt = (SpillingAdaptiveSpanningRecordDeserializer) currentRecordDeserializer;
					byte[] b = spAdapt.getNonSpanningBytes();
					totalBytes.add(b);
				}

			//	DeserializationResult result = currentRecordDeserializer.getNextRecord(target);

			//	if (result.isBufferConsumed()) {
					//System.out.println(">>>>> Buffer consumed!");
					final Buffer currentBuffer = currentRecordDeserializer.getCurrentBuffer();

					currentBuffer.recycleBuffer();
					currentRecordDeserializer = null;
			//	}

			//	if (result.isFullRecord()) {
					//System.out.println(">>>>> Full record!");
					return true;
			//	}
			}

			final BufferOrEvent bufferOrEvent = inputGate.getNext().orElseThrow(IllegalStateException::new);

			if (bufferOrEvent.isBuffer()) {
				//System.out.println(">>>>> isBuffer!");
				currentRecordDeserializer = recordDeserializers.get(bufferOrEvent.getChannelInfo());
				currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
			}
			else {
				//System.out.println(">>>>> isEvent!");
				// sanity check for leftover data in deserializers. events should only come between
				// records, not in the middle of a fragment
				if (recordDeserializers.get(bufferOrEvent.getChannelInfo()).hasUnfinishedData()) {
					throw new IOException(
						"Received an event in channel " + bufferOrEvent.getChannelInfo() + " while still having "
							+ "data from a record. This indicates broken serialization logic. "
							+ "If you are using custom serialization code (Writable or Value types), check their "
							+ "serialization routines. In the case of Kryo, check the respective Kryo serializer.");
				}

				if (handleEvent(bufferOrEvent.getEvent())) {
					//System.out.println(">>>>> handledEvent!");
					if (inputGate.isFinished()) {
						//System.out.println(">>>>> inputGate finished!");
						StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
						//System.out.println("----- I was called by a method named: " + Arrays.toString(stackTrace));
						isFinished = true;
						return false;
					}
					else if (hasReachedEndOfSuperstep()) {
						//System.out.println(">>>>> End of superstep!");
						return false;
					}
					// else: More data is coming...
				}
			}
		}
	}


	public byte[] getSpillingAdaptiveBytes() {
		ArrayList<byte[]> totalBytes = new ArrayList<>();
		try {
//			if (currentRecordDeserializer == null) {
//				if (!requestedPartitions) {
//					inputGate.requestPartitions();
//					requestedPartitions = true;
//				}
//
//				final BufferOrEvent bufferOrEvent = inputGate.getNext().orElseThrow(IllegalStateException::new);
//
//				if (bufferOrEvent.isBuffer()) {
//					currentRecordDeserializer = recordDeserializers.get(bufferOrEvent.getChannelInfo());
//					currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
//				}
//			
			if (!requestedPartitions) {
			    //System.out.println("Initial request for partitions");
			    inputGate.requestPartitions();
		    	    requestedPartitions = true;	    
			} /*else {
			    System.out.println("Initial request for partitions, partitions already requested");
			}*/
			while (!isFinished && !endOfSuperStep) {
			//	System.out.println("--- requestedPartitions: " + requestedPartitions);
				if (!requestedPartitions) {
					inputGate.requestPartitions();
					requestedPartitions = true;
				}

				while (true) {
					if (currentRecordDeserializer != null) {
					//	System.out.println("--- currentRecordDeserializer: " + currentRecordDeserializer);
						if (currentRecordDeserializer instanceof SpillingAdaptiveSpanningRecordDeserializer) {
							//System.out.println("--- record deserializer spillingadaptive");
							SpillingAdaptiveSpanningRecordDeserializer spAdapt = (SpillingAdaptiveSpanningRecordDeserializer) currentRecordDeserializer;
							byte[] b = spAdapt.getNonSpanningBytes();
							/*if (b == null) {
							   System.out.println("--- record read is null");
							}*/
							totalBytes.add(b);
						}

						final Buffer currentBuffer = currentRecordDeserializer.getCurrentBuffer();

						currentBuffer.recycleBuffer();
						currentRecordDeserializer = null;
					}

					final BufferOrEvent bufferOrEvent = inputGate.getNext().orElseThrow(IllegalStateException::new);
					//System.out.println("--- bufferOrEvent: " + bufferOrEvent);

					if (bufferOrEvent.isBuffer()) {
					//	System.out.println("--- isBuffer");
						currentRecordDeserializer = recordDeserializers.get(bufferOrEvent.getChannelInfo());
						currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
					//	System.out.println("--- currentRecordDeserializer: " + currentRecordDeserializer);
					} else {
						if (recordDeserializers.get(bufferOrEvent.getChannelInfo()).hasUnfinishedData()) {
							throw new IOException(
								"Received an event in channel " + bufferOrEvent.getChannelInfo() + " while still having "
									+ "data from a record. This indicates broken serialization logic. "
									+ "If you are using custom serialization code (Writable or Value types), check their "
									+ "serialization routines. In the case of Kryo, check the respective Kryo serializer.");
						}

						if (handleEvent(bufferOrEvent.getEvent())) {
							if (inputGate.isFinished()) {
								System.out.println("--- isEvent: isFinished=true");
								isFinished = true;
								break;
							}
							else if (hasReachedEndOfSuperstep()) {
								System.out.println("--- isEvent: endofSuperstep!");
								//return null;
								//isFinished = true;

								endOfSuperStep = true;
								break;
							}
							// else: More data is coming...
						}
					}
				}
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
		} catch (Exception e) {
			System.out.println("Exception in AbstractRecordReader");
			return null;
		}
	}

	protected boolean getNextRecord(T target) throws IOException, InterruptedException {
		// The action of partition request was removed from InputGate#setup since FLINK-16536, and this is the only
		// unified way for launching partition request for batch jobs. In order to avoid potential performance concern,
		// we might consider migrating this action back to the setup based on some condition judgement future.
		if (!requestedPartitions) {
			inputGate.requestPartitions();
			requestedPartitions = true;
			//System.out.println(">>>>> Partitions requested!");
		}

		if (isFinished) {
			//System.out.println(">>>>> Finished, return false!");
			return false;
		}

		while (true) {
			if (currentRecordDeserializer != null) {
				//System.out.println(">>>>> currentRecordDeserializer is not null!");
				DeserializationResult result = currentRecordDeserializer.getNextRecord(target);

				if (result.isBufferConsumed()) {
					//System.out.println(">>>>> Buffer consumed!");
					final Buffer currentBuffer = currentRecordDeserializer.getCurrentBuffer();

					currentBuffer.recycleBuffer();
					currentRecordDeserializer = null;
				}

				if (result.isFullRecord()) {
					//System.out.println(">>>>> Full record!");
					return true;
				}
			}

			final BufferOrEvent bufferOrEvent = inputGate.getNext().orElseThrow(IllegalStateException::new);

			if (bufferOrEvent.isBuffer()) {
				//System.out.println(">>>>> isBuffer!");
				currentRecordDeserializer = recordDeserializers.get(bufferOrEvent.getChannelInfo());
				currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
			}
			else {
				//System.out.println(">>>>> isEvent!");
				// sanity check for leftover data in deserializers. events should only come between
				// records, not in the middle of a fragment
				if (recordDeserializers.get(bufferOrEvent.getChannelInfo()).hasUnfinishedData()) {
					throw new IOException(
							"Received an event in channel " + bufferOrEvent.getChannelInfo() + " while still having "
							+ "data from a record. This indicates broken serialization logic. "
							+ "If you are using custom serialization code (Writable or Value types), check their "
							+ "serialization routines. In the case of Kryo, check the respective Kryo serializer.");
				}

				if (handleEvent(bufferOrEvent.getEvent())) {
					//System.out.println(">>>>> handledEvent!");
					if (inputGate.isFinished()) {
						//System.out.println(">>>>> inputGate finished!");
						StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
						//System.out.println("----- I was called by a method named: " + Arrays.toString(stackTrace));
						isFinished = true;
						return false;
					}
					else if (hasReachedEndOfSuperstep()) {
						//System.out.println(">>>>> End of superstep!");
						return false;
					}
					// else: More data is coming...
				}
			}
		}
	}

	public void clearBuffers() {
		for (RecordDeserializer<?> deserializer : recordDeserializers.values()) {
			Buffer buffer = deserializer.getCurrentBuffer();
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycleBuffer();
			}
			deserializer.clear();
		}
	}
}
