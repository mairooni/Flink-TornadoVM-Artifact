package org.apache.flink.runtime.tornadovm;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.NumericTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.io.TypeSerializerInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.util.TornadoFlinkTypeRuntimeException;
import uk.ac.manchester.tornado.api.flink.FlinkCompilerInfo;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

public class DataTransformation<T> {

	private static int numberOfArrayElementsStored;
	private static int numberOfElementsStored;

	public AccelerationData generateInputAccelerationData(byte[] inputDataBytes, TypeSerializer ser, ArrayList ar, boolean DataSourceTask, boolean reuse, boolean header, boolean iterations) {

		Object record = ser.createInstance();
		int numberOfElements = 0;
		int bytesToAllocate = 0;
		TypeInformation<?> type = TypeExtractor.getForObject(record);
		boolean padding = false;
		int arrayFieldNo = -1;
		int numberOfArrayElements = 0;
		int arrayElementSize = 0;
		boolean tuplearray = false;
		int recordSize = 0;
		int sizeOfTuple = 0;
		int rowBytes = 0;
		int rowSize = 0;
		int columnSize = 0;
		byte[] rev = null;
		ArrayList<Integer> actualSizes = new ArrayList<>();
		if (type.getClass() == TupleTypeInfo.class) {
			TupleTypeInfo tinfo = (TupleTypeInfo) type;
			TypeInformation[] tuparray = tinfo.getTypeArray();
			boolean genericTuple = false;
			String arrayFieldType = null;

			int[] tupleFieldSize = new int[tinfo.getTotalFields()];
			int k = 0;
			boolean nested = false;
			int nestedPos = 0;
			boolean nestedArray = false;
			int nestedSize = 0;
			for (int i = 0; i < tupleFieldSize.length; i++) {
				Class typeClass = tuparray[k].getTypeClass();
				if (typeClass.toString().contains("Double")) {
					tupleFieldSize[i] = 8;
					actualSizes.add(8);
					k++;
				} else if (typeClass.toString().contains("[")) {
					arrayFieldType = typeClass.toString();
					tuplearray = true;
					arrayFieldNo = i;
					actualSizes.add(0);
					k++;
				} else if (typeClass.toString().contains("Float")) {
					tupleFieldSize[i] = 4;
					actualSizes.add(4);
					k++;
				} else if (typeClass.toString().contains("Long")) {
					tupleFieldSize[i] = 8;
					actualSizes.add(8);
					k++;
				} else if (typeClass.toString().contains("Integer")) {
					tupleFieldSize[i] = 4;
					actualSizes.add(4);
					k++;
				} else if (typeClass.toString().contains("Tuple")) {
					nested = true;
					nestedPos = i;
					TypeInformation[] nestedtuparray = ((TupleTypeInfo) tuparray[k]).getTypeArray();
					for (int j = 0; j < nestedtuparray.length; j++) {
						Class nestedTypeClass = nestedtuparray[j].getTypeClass();
						if (nestedTypeClass.toString().contains("Double")) {
							tupleFieldSize[i + j] = 8;
							actualSizes.add(8);
						} else if (nestedTypeClass.toString().contains("[")) {
							arrayFieldType = nestedTypeClass.toString();
							tuplearray = true;
							arrayFieldNo = i + j;
							nestedArray = true;
							actualSizes.add(0);
						} else if (nestedTypeClass.toString().contains("Float")) {
							tupleFieldSize[i + j] = 4;
							actualSizes.add(4);
						} else if (nestedTypeClass.toString().contains("Long")) {
							tupleFieldSize[i + j] = 8;
							actualSizes.add(8);
						} else if (nestedTypeClass.toString().contains("Integer")) {
							tupleFieldSize[i + j] = 4;
							actualSizes.add(4);
						}
					}
					nestedSize = nestedtuparray.length;
					i += (nestedtuparray.length - 1);
					k++;
				}
			}

			if (nested) {
				if (nestedArray && nestedSize > 2) {
					System.out.println("WARNING: AT THE MOMENT WE ONLY SUPPORT NESTED TUPLE2S THAT CONTAIN AN ARRAY");
				}
			}

			if (DataSourceTask) {
				int tupSize;
				if (nested) {
					tupSize = tuparray.length;
				} else {
					tupSize = tupleFieldSize.length;
				}

				if (tuplearray) {
					if (tupSize == 2) {
						if (nested) {
							if (nestedSize == 2) {
								Tuple2 t = (Tuple2) ar.get(0);
								if (nestedPos == 0) {
									Tuple2 nT = (Tuple2) t.f0;
									numberOfArrayElements = getArraySizeTuple2(arrayFieldNo, arrayFieldType, nT, actualSizes);
								} else {
									Tuple2 nT = (Tuple2) t.f1;
									numberOfArrayElements = getArraySizeTuple2(arrayFieldNo, arrayFieldType, nT, actualSizes);
								}
							}
						} else {
							Tuple2 t = (Tuple2) ar.get(0);
							numberOfArrayElements = getArraySizeTuple2(arrayFieldNo, arrayFieldType, t, actualSizes);
						}
					} else if (tupSize == 3) {
						Tuple3 t = (Tuple3) ar.get(0);
						numberOfArrayElements = getArraySizeTuple3(arrayFieldNo, arrayFieldType, t, actualSizes);
					} else if (tupSize == 4) {
						Tuple4 t = (Tuple4) ar.get(0);
						numberOfArrayElements = getArraySizeTuple4(arrayFieldNo, arrayFieldType, t, actualSizes);
					} else {
						System.out.println("[ERROR] FLINK-TORNADOVM INTEGRATION CURRENTLY SUPPORTS UP TO TUPLE4");
					}
				}
			} else {
				if (tuplearray && !reuse) {

					int bytesToSkip = 4;
					for (int i = 0; i < arrayFieldNo; i++) {
						bytesToSkip += tupleFieldSize[i];
					}

					byte[] arraySizeInBytes = new byte[4];
					int w = 0;
					//for (int i = 0; i < 4; i++) {
					for (int i = bytesToSkip; i < bytesToSkip + 4; i++) {
						arraySizeInBytes[w] = inputDataBytes[i];
						w++;
					}
					
					numberOfArrayElements = ByteBuffer.wrap(arraySizeInBytes).getInt();
					if (iterations) {
					    numberOfArrayElementsStored = numberOfArrayElements;
					}
				} else if (tuplearray) {
					numberOfArrayElements = numberOfArrayElementsStored;
					
				}
			}

			if (tuplearray) {
				arrayElementSize = getSizeOfArrayField(arrayFieldType);

				for (int i = 0; i < actualSizes.size(); i++) {
					for (int j = i + 1; j < actualSizes.size(); j++) {
						if (i == arrayFieldNo) {
							if (arrayElementSize != actualSizes.get(j)) {
								padding = true;
								break;
							}
						} else if (j == arrayFieldNo) {
							if (arrayElementSize != actualSizes.get(i)) {
								padding = true;
								break;
							}
						} else {
							if (!actualSizes.get(i).equals(actualSizes.get(j))) {
								padding = true;
								break;
							}
						}
					}
				}

				int arrayFieldTotalBytes = numberOfArrayElements * arrayElementSize;
				tupleFieldSize[arrayFieldNo] = arrayFieldTotalBytes;

				if (padding) {
					AbstractInvokable.setTupleArrayFieldTotalBytes(numberOfArrayElements * 8);
				} else {
					AbstractInvokable.setTupleArrayFieldTotalBytes(arrayFieldTotalBytes);
				}
			}

			if (!tuplearray) {
				for (int i = 0; i < tupleFieldSize.length; i++) {
					for (int j = i + 1; j < tupleFieldSize.length; j++) {
						if (tupleFieldSize[i] != tupleFieldSize[j]) {
							padding = true;
							break;
						}
					}
				}
			}


			for (int i = 0; i < tupleFieldSize.length; i++) {
				sizeOfTuple += tupleFieldSize[i];
			}

			int rawBytes;
			//boolean powOf2 = true;
			int actualNumOfElements = 0;
			if (DataSourceTask) {
				numberOfElements = ar.size();
				rawBytes = sizeOfTuple * numberOfElements;
				actualNumOfElements = numberOfElements;
			} else {
				if (reuse) {
					numberOfElements = numberOfElementsStored;
					rawBytes = sizeOfTuple * numberOfElements;
					actualNumOfElements = numberOfElements;
				} else {
					if (tuplearray) {
						int recordBytes = 4 + 4 + sizeOfTuple;
						numberOfElements = inputDataBytes.length / recordBytes;
					} else {
						int recordBytes;
						if (header) {
							recordBytes = 4 + sizeOfTuple;
						} else {
							recordBytes = sizeOfTuple;
						}
						numberOfElements = inputDataBytes.length / recordBytes;
					}
					rawBytes = sizeOfTuple * numberOfElements;
					actualNumOfElements = numberOfElements;
//					double log = Math.log(numberOfElements) / Math.log(2);
//					int exp = (int) log;
//					if (Math.log(numberOfElements) % Math.log(2) != 0) {
//						powOf2 = false;
//						numberOfElements = (int) Math.pow(2, exp + 1);
//					} else {
//						numberOfElements = (int) Math.pow(2, exp);
//					}
					if (iterations) {					
					    numberOfElementsStored = numberOfElements;
					}
				}
			}

			//int rawBytes = sizeOfTuple * numberOfElements;
			if (padding) {
				if (tuplearray) {
					// this is correct if only one of the fields of the tuple is an array
					bytesToAllocate = ((tupleFieldSize.length - 1) * 8 + numberOfArrayElements * 8) * numberOfElements;
					recordSize = ((tupleFieldSize.length - 1) * 8 + numberOfArrayElements * 8);
				} else {
					bytesToAllocate = (tupleFieldSize.length * 8) * numberOfElements;
				}
			} else {
				//if (!powOf2) {
				//	bytesToAllocate = numberOfElements*sizeOfTuple;
				//	recordSize = sizeOfTuple;
				//} else {
					bytesToAllocate = rawBytes;
					recordSize = sizeOfTuple;
				//}

			}
			if (!reuse) {
				if (DataSourceTask) {
					rev = getTupleByteArrayDataSource(numberOfElements, inputDataBytes, bytesToAllocate, rawBytes, tupleFieldSize, arrayFieldNo, numberOfArrayElements, arrayElementSize, padding);
				} else {
					rev = getTupleByteArray(actualNumOfElements, inputDataBytes, bytesToAllocate, tupleFieldSize, arrayFieldNo, numberOfArrayElements, arrayElementSize, padding, header);
				}
			}
		} else {
			int elementTypeSize = getTypeBytesNumericType(type);
			if (DataSourceTask) {
				numberOfElements = ar.size();
			} else {
				if (type instanceof BasicArrayTypeInfo) {
					byte[] rowNumOfBytes = new byte[4];
					for (int i = 0; i < 4; i++) {
						rowNumOfBytes[i] = inputDataBytes[i];
					}
					byte[] rowNumOfElements = new byte[4];
					for (int i = 0; i < 4; i++) {
						rowNumOfElements[i] = inputDataBytes[i + 4];
					}
					rowBytes = ByteBuffer.wrap(rowNumOfBytes).getInt();
					columnSize = ByteBuffer.wrap(rowNumOfElements).getInt();
					rowSize = inputDataBytes.length / (rowBytes + 4);
					numberOfElements = rowSize * columnSize;
				} else {
					numberOfElements = inputDataBytes.length / (elementTypeSize + 4);
				}
			}
			bytesToAllocate = elementTypeSize * numberOfElements;
			if (!reuse) {
				if (DataSourceTask) {
					rev = getByteArrayDataSource(elementTypeSize, inputDataBytes, numberOfElements, bytesToAllocate);
				} else {
					if (type instanceof BasicArrayTypeInfo) {
						rev = getMatrixByteArray(elementTypeSize, inputDataBytes, bytesToAllocate, rowBytes, columnSize, rowSize);
//						System.out.println("REVERSED:");
//						for (int i = 0; i < rev.length; i++) {
//							System.out.print(rev[i] + " ");
//						}
//						System.out.println("\n");
					} else {
						rev = getByteArray(elementTypeSize, inputDataBytes, numberOfElements, bytesToAllocate);
					}
				}
			}
		}

		AccelerationData acdata;
		if (!reuse) {
			acdata = new AccelerationData(rev, numberOfElements, type);
		} else {
			acdata = new AccelerationData(inputDataBytes, numberOfElements, type);
		}

		if (type instanceof BasicArrayTypeInfo) {
			acdata.setRowSize(rowSize);
			acdata.setColumnSize(columnSize);
		}

		if (tuplearray) {
			acdata.hasArrayField();
			acdata.setLengthOfArrayField(numberOfArrayElements);
			acdata.setArrayFieldNo(arrayFieldNo);
			acdata.setTotalBytes(sizeOfTuple);
			acdata.setRecordSize(recordSize);
		}

		return acdata;
	}

	public AccelerationData generateBroadcastedInputAccelerationData(ArrayList ar, TypeSerializer ser) {
		Object record;
		if (ser == null) {
			record = ar.get(0);
		} else {
			record = ser.createInstance();
		}
		int numberOfElements = 0;
		int bytesToAllocate = 0;
		TypeInformation<?> type = TypeExtractor.getForObject(record);
		boolean padding = false;
		int arrayFieldNo = -1;
		int numberOfArrayElements = 0;
		int arrayElementSize = 0;
		boolean tuplearray = false;
		int recordSize = 0;
		int sizeOfTuple = 0;
		byte[] rev = null;
		boolean matrix = false;

		if (type.getClass() == TupleTypeInfo.class) {
			TupleTypeInfo tinfo = (TupleTypeInfo) type;
			TypeInformation[] tuparray = tinfo.getTypeArray();
			boolean genericTuple = false;
			String arrayFieldType = null;
			ArrayList<Integer> actualSizes = new ArrayList<>();
			int[] tupleFieldSize = new int[tinfo.getTotalFields()];
			int k = 0;
			boolean nested = false;
			int nestedPos = 0;
			boolean nestedArray = false;
			int nestedSize = 0;
			for (int i = 0; i < tupleFieldSize.length; i++) {
				Class typeClass = tuparray[k].getTypeClass();
				if (typeClass.toString().contains("Double")) {
					tupleFieldSize[i] = 8;
					actualSizes.add(8);
					k++;
				} else if (typeClass.toString().contains("[")) {
					arrayFieldType = typeClass.toString();
					tuplearray = true;
					arrayFieldNo = i;
					k++;
				} else if (typeClass.toString().contains("Float")) {
					tupleFieldSize[i] = 4;
					actualSizes.add(4);
					k++;
				} else if (typeClass.toString().contains("Long")) {
					tupleFieldSize[i] = 8;
					actualSizes.add(8);
					k++;
				} else if (typeClass.toString().contains("Integer")) {
					tupleFieldSize[i] = 4;
					actualSizes.add(4);
					k++;
				} else if (typeClass.toString().contains("Tuple")) {
					nested = true;
					nestedPos = i;
					TypeInformation[] nestedtuparray = ((TupleTypeInfo) tuparray[k]).getTypeArray();
					for (int j = 0; j < nestedtuparray.length; j++) {
						Class nestedTypeClass = nestedtuparray[j].getTypeClass();
						if (nestedTypeClass.toString().contains("Double")) {
							tupleFieldSize[i + j] = 8;
							actualSizes.add(8);
						} else if (nestedTypeClass.toString().contains("[")) {
							arrayFieldType = nestedTypeClass.toString();
							tuplearray = true;
							arrayFieldNo = i + j;
							nestedArray = true;
						} else if (nestedTypeClass.toString().contains("Float")) {
							tupleFieldSize[i + j] = 4;
							actualSizes.add(4);
						} else if (nestedTypeClass.toString().contains("Long")) {
							tupleFieldSize[i + j] = 8;
							actualSizes.add(8);
						} else if (nestedTypeClass.toString().contains("Integer")) {
							tupleFieldSize[i + j] = 4;
							actualSizes.add(4);
						}
					}
					nestedSize = nestedtuparray.length;
					i += (nestedtuparray.length - 1);
					k++;
				}
			}

			if (nested) {
				if (nestedArray && nestedSize > 2) {
					System.out.println("WARNING: AT THE MOMENT WE ONLY SUPPORT NESTED TUPLE2S THAT CONTAIN AN ARRAY");
				}
			}


			int tupSize;
			if (nested) {
				tupSize = tuparray.length;
			} else {
				tupSize = tupleFieldSize.length;
			}

			if (tuplearray) {
				if (tupSize == 2) {
					if (nested) {
						if (nestedSize == 2) {
							Tuple2 t = (Tuple2) ar.get(0);
							if (nestedPos == 0) {
								Tuple2 nT = (Tuple2) t.f0;
								numberOfArrayElements = getArraySizeTuple2(arrayFieldNo, arrayFieldType, nT, actualSizes);
							} else {
								Tuple2 nT = (Tuple2) t.f1;
								numberOfArrayElements = getArraySizeTuple2(arrayFieldNo, arrayFieldType, nT, actualSizes);
							}
						}
					} else {
						Tuple2 t = (Tuple2) ar.get(0);
						numberOfArrayElements = getArraySizeTuple2(arrayFieldNo, arrayFieldType, t, actualSizes);
					}
				} else if (tupSize == 3) {
					Tuple3 t = (Tuple3) ar.get(0);
					numberOfArrayElements = getArraySizeTuple3(arrayFieldNo, arrayFieldType, t, actualSizes);
				} else if (tupSize == 4) {
					Tuple4 t = (Tuple4) ar.get(0);
					numberOfArrayElements = getArraySizeTuple4(arrayFieldNo, arrayFieldType, t, actualSizes);
				} else {
					System.out.println("[ERROR] FLINK-TORNADOVM INTEGRATION CURRENTLY SUPPORTS UP TO TUPLE4");
				}
			}

			if (tuplearray) {
				arrayElementSize = getSizeOfArrayField(arrayFieldType);
				for (int i = 0; i < actualSizes.size(); i++) {
					for (int j = i + 1; j < actualSizes.size(); j++) {
						if (i == arrayFieldNo) {
							if (arrayElementSize != actualSizes.get(j)) {
								padding = true;
								break;
							}
						} else if (j == arrayFieldNo) {
							if (arrayElementSize != actualSizes.get(i)) {
								padding = true;
								break;
							}
						} else {
							if (!actualSizes.get(i).equals(actualSizes.get(j))) {
								padding = true;
								break;
							}
						}
					}
				}


				int arrayFieldTotalBytes = numberOfArrayElements * arrayElementSize;
				tupleFieldSize[arrayFieldNo] = arrayFieldTotalBytes;

				if (padding) {
					AbstractInvokable.setTupleArrayFieldTotalBytes(numberOfArrayElements * 8);
				} else {
					AbstractInvokable.setTupleArrayFieldTotalBytes(arrayFieldTotalBytes);
				}
			}

			if (!tuplearray) {
				for (int i = 0; i < tupleFieldSize.length; i++) {
					for (int j = i + 1; j < tupleFieldSize.length; j++) {
						if (tupleFieldSize[i] != tupleFieldSize[j]) {
							padding = true;
							break;
						}
					}
				}
			}


			for (int i = 0; i < tupleFieldSize.length; i++) {
				sizeOfTuple += tupleFieldSize[i];
			}


			numberOfElements = ar.size();


			int rawBytes = sizeOfTuple * numberOfElements;
			if (padding) {
				if (tuplearray) {
					// this is correct if only one of the fields of the tuple is an array
					bytesToAllocate = ((tupleFieldSize.length - 1) * 8 + numberOfArrayElements * 8) * numberOfElements;
					recordSize = ((tupleFieldSize.length - 1) * 8 + numberOfArrayElements * 8);
				} else {
					bytesToAllocate = (tupleFieldSize.length * 8) * numberOfElements;
				}
			} else {
				bytesToAllocate = rawBytes;
				recordSize = sizeOfTuple;
			}

			rev = getTupleByteArrayBroadcasted(numberOfElements, ar, bytesToAllocate, tuparray, arrayFieldNo, numberOfArrayElements, arrayElementSize, padding);

		} else {
			int elementTypeSize = getTypeBytesNumericType(type);

			if (type instanceof BasicArrayTypeInfo) {
				matrix = true;
				numberOfElements = ar.size() * arraySize(type, ar);
				bytesToAllocate = elementTypeSize * numberOfElements;
				rev = getMatrixByteArrayBroadcasted(ar, bytesToAllocate, elementTypeSize, type);
			} else {
				numberOfElements = ar.size();

				bytesToAllocate = elementTypeSize * numberOfElements;
				rev = getByteArrayBroadcasted(ar, bytesToAllocate, type);
//				rev = getByteArrayDataSource(elementTypeSize, inputDataBytes, numberOfElements, bytesToAllocate);
//			} else {
//				rev = getByteArray(elementTypeSize, inputDataBytes, numberOfElements, bytesToAllocate);
//			}
			}
		}

		AccelerationData acdata;
		acdata = new AccelerationData(rev, numberOfElements, type);

		if (tuplearray) {
			acdata.hasArrayField();
			acdata.setLengthOfArrayField(numberOfArrayElements);
			acdata.setArrayFieldNo(arrayFieldNo);
			acdata.setTotalBytes(sizeOfTuple);
			acdata.setRecordSize(recordSize);
		}
		if (matrix) {
			int rowSize = ar.size();
			int columnSize = ((Object[])ar.get(0)).length;
			acdata.setRowSize(rowSize);
			acdata.setColumnSize(columnSize);
		}

		return acdata;
	}

	public ArrayList<T> materializeRecords(String type, byte[] b, FlinkCompilerInfo fc, boolean padding, int recordNumOfBytes, int actualSize) {
		ArrayList<T> records = new ArrayList<>();
		if (fc.getReturnTuple()) {
			if (type.contains("Tuple3")) {
				// for each tuple
				int j = 0;

				for (int i = 0; i < b.length; i += recordNumOfBytes) {
					//type, fct, padding, out, actualSize, i);
					Tuple3 t = new Tuple3();
					byte[] rec = this.getTupleByteRecord(type, fc, padding, b, actualSize, i);
					int fieldNum = 0;
					int numOfFields = fc.getFieldTypesRetCopy().size();
					int tpos = 0;
					while (fieldNum < numOfFields) {
						String fieldType = fc.getFieldTypesRetCopy().get(fieldNum);
						if (fc.getNestedTuples()) {
							if (fieldNum == fc.getNestedTupleField()) {
								int sizeOfNested = fc.getSizeOfNestedTuple();
								if (sizeOfNested == 2) {
									Tuple2 nestedT = new Tuple2();
									for (int k = 0; k < 2; k++) {
										if (fieldType.contains("int")) {
											byte[] intFieldB = Arrays.copyOfRange(rec, j, j + 4);
											j += 4;
											Integer intField = ByteBuffer.wrap(intFieldB).getInt();
											if (k == 0) nestedT.f0 = intField;
											else if (k == 1) nestedT.f1 = intField;
										} else if (fieldType.contains("double")) {
											byte[] doubleFieldB = Arrays.copyOfRange(rec, j, j + 8);
											j += 8;
											Double doubleField = ByteBuffer.wrap(doubleFieldB).getDouble();
											if (k == 0) nestedT.f0 = doubleField;
											else if (k == 1) nestedT.f1 = doubleField;
										} else if (fieldType.contains("long")) {
											byte[] longFieldB = Arrays.copyOfRange(rec, j, j + 8);
											j += 8;
											Long longField = ByteBuffer.wrap(longFieldB).getLong();
											if (k == 0) nestedT.f0 = longField;
											else if (k == 1) nestedT.f1 = longField;
										}
									}
									if (tpos == 0) t.f0 = nestedT;
									else if (tpos == 1) t.f1 = nestedT;
									else if (tpos == 2) t.f2 = nestedT;
									fieldNum += 2;
								}
							} else {
								if (fieldType.contains("int")) {
									byte[] intFieldB = Arrays.copyOfRange(rec, j, j + 4);
									j += 4;
									Integer intField = ByteBuffer.wrap(intFieldB).getInt();
									if (tpos == 0) t.f0 = intField;
									else if (tpos == 1) t.f1 = intField;
									else if (tpos == 2) t.f2 = intField;
								} else if (fieldType.contains("double")) {
									byte[] doubleFieldB = Arrays.copyOfRange(rec, j, j + 8);
									j += 8;
									Double doubleField = ByteBuffer.wrap(doubleFieldB).getDouble();
									if (tpos == 0) t.f0 = doubleField;
									else if (tpos == 1) t.f1 = doubleField;
									else if (tpos == 2) t.f2 = doubleField;
								} else if (fieldType.contains("long")) {
									byte[] longFieldB = Arrays.copyOfRange(rec, j, j + 8);
									j += 8;
									Long longField = ByteBuffer.wrap(longFieldB).getLong();
									if (tpos == 0) t.f0 = longField;
									else if (tpos == 1) t.f1 = longField;
									else if (tpos == 2) t.f2 = longField;
								}
								fieldNum++;
							}
							tpos++;
						}
					}
					records.add((T) t);
					j = 0;
				}
			}
		}
		return records;
	}

	public ArrayList<T> materializeInputRecords(String type, byte[] b, FlinkCompilerInfo fc, boolean padding, int recordNumOfBytes, int actualSize) {
		ArrayList<T> records = new ArrayList<>();
		if (fc.getHasTuples()) {
			if (type.contains("Tuple2")) {
				// for each tuple
				int j = 0;

				for (int i = 0; i < b.length; i += recordNumOfBytes) {
					//type, fct, padding, out, actualSize, i);
					Tuple2 t = new Tuple2();
					byte[] rec = this.getInputTupleByteRecord(type, fc, padding, b, actualSize, i);
					int fieldNum = 0;
					int numOfFields = fc.getFieldTypes().size();
					int tpos = 0;
					while (fieldNum < numOfFields) {
						if (fc.getNestedTuples()) {
							if (fieldNum == fc.getNestedTupleField()) {
								int sizeOfNested = fc.getSizeOfNestedTuple();
								if (sizeOfNested == 2) {
									Tuple2 nestedT = new Tuple2();
									for (int k = 0; k < 2; k++) {
										String fieldType = fc.getFieldTypes().get(fieldNum);
										if (fieldType.contains("int")) {
											byte[] intFieldB;
											// TODO: This is a hack, implement it properly
											if (fc.getArrayField()) {
												intFieldB = Arrays.copyOfRange(rec, j, j + 8);
												j += 8;
											} else {
												intFieldB = Arrays.copyOfRange(rec, j, j + 4);
												j += 4;
											}
											Integer intField = ByteBuffer.wrap(intFieldB).getInt();
											if (k == 0) nestedT.f0 = intField;
											else nestedT.f1 = intField;
											fieldNum++;
										} else if (fieldType.contains("double")) {
											if (fc.getArrayField()) {
												double[] doubleArrayField = new double[fc.getArraySize()];
												int q = 0;
												while (j < fc.getArrayFieldTotalBytes()) {
													byte[] doubleFieldB = Arrays.copyOfRange(rec, j, j + 8);
													j += 8;
													Double doubleField = ByteBuffer.wrap(doubleFieldB).getDouble();
													doubleArrayField[q] = doubleField;
													q++;
												}
												if (k == 0) nestedT.f0 = doubleArrayField;
												else nestedT.f1 = doubleArrayField;
												fieldNum++;
											} else {
												byte[] doubleFieldB = Arrays.copyOfRange(rec, j, j + 8);
												j += 8;
												Double doubleField = ByteBuffer.wrap(doubleFieldB).getDouble();
												if (k == 0) nestedT.f0 = doubleField;
												else nestedT.f1 = doubleField;
												fieldNum++;
											}
										} else if (fieldType.contains("long")) {
											byte[] longFieldB = Arrays.copyOfRange(rec, j, j + 8);
											j += 8;
											Long longField = ByteBuffer.wrap(longFieldB).getLong();
											if (k == 0) nestedT.f0 = longField;
											else nestedT.f1 = longField;
											fieldNum++;
										}
									}
									if (tpos == 0) t.f0 = nestedT;
									else if (tpos == 1) t.f1 = nestedT;
									//fieldNum += 2;
								}
							} else {
								String fieldType = fc.getFieldTypes().get(fieldNum);
								if (fieldType.contains("int")) {
									byte[] intFieldB;
									// TODO: This is a hack, implement it properly
									if (fc.getArrayField()) {
										intFieldB = Arrays.copyOfRange(rec, j, j + 8);
										j += 8;
									} else {
										intFieldB = Arrays.copyOfRange(rec, j, j + 4);
										j += 4;
									}
									Integer intField = ByteBuffer.wrap(intFieldB).getInt();
									if (tpos == 0) t.f0 = intField;
									else if (tpos == 1) t.f1 = intField;
								} else if (fieldType.contains("double")) {
									byte[] doubleFieldB = Arrays.copyOfRange(rec, j, j + 8);
									j += 8;
									Double doubleField = ByteBuffer.wrap(doubleFieldB).getDouble();
									if (tpos == 0) t.f0 = doubleField;
									else if (tpos == 1) t.f1 = doubleField;
								} else if (fieldType.contains("long")) {
									byte[] longFieldB = Arrays.copyOfRange(rec, j, j + 8);
									j += 8;
									Long longField = ByteBuffer.wrap(longFieldB).getLong();
									if (tpos == 0) t.f0 = longField;
									else if (tpos == 1) t.f1 = longField;
								}
								fieldNum++;
							}
							tpos++;
						}
					}
					records.add((T) t);
					j = 0;
				}
			} else if (type.contains("Tuple3")) {
				// for each tuple
				int j = 0;

				for (int i = 0; i < b.length; i += recordNumOfBytes) {
					//type, fct, padding, out, actualSize, i);
					Tuple3 t = new Tuple3();
					byte[] rec = this.getTupleByteRecord(type, fc, padding, b, actualSize, i);
					int fieldNum = 0;
					int numOfFields = fc.getFieldTypesRetCopy().size();
					int tpos = 0;
					while (fieldNum < numOfFields) {
						String fieldType = fc.getFieldTypesRetCopy().get(fieldNum);
						if (fc.getNestedTuples()) {
							if (fieldNum == fc.getNestedTupleField()) {
								int sizeOfNested = fc.getSizeOfNestedTuple();
								if (sizeOfNested == 2) {
									Tuple2 nestedT = new Tuple2();
									for (int k = 0; k < 2; k++) {
										if (fieldType.contains("int")) {
											byte[] intFieldB = Arrays.copyOfRange(rec, j, j + 4);
											j += 4;
											Integer intField = ByteBuffer.wrap(intFieldB).getInt();
											if (k == 0) nestedT.f0 = intField;
											else if (k == 1) nestedT.f1 = intField;
										} else if (fieldType.contains("double")) {
											byte[] doubleFieldB = Arrays.copyOfRange(rec, j, j + 8);
											j += 8;
											Double doubleField = ByteBuffer.wrap(doubleFieldB).getDouble();
											if (k == 0) nestedT.f0 = doubleField;
											else if (k == 1) nestedT.f1 = doubleField;
										} else if (fieldType.contains("long")) {
											byte[] longFieldB = Arrays.copyOfRange(rec, j, j + 8);
											j += 8;
											Long longField = ByteBuffer.wrap(longFieldB).getLong();
											if (k == 0) nestedT.f0 = longField;
											else if (k == 1) nestedT.f1 = longField;
										}
									}
									if (tpos == 0) t.f0 = nestedT;
									else if (tpos == 1) t.f1 = nestedT;
									else if (tpos == 2) t.f2 = nestedT;
									fieldNum += 2;
								}
							} else {
								if (fieldType.contains("int")) {
									byte[] intFieldB = Arrays.copyOfRange(rec, j, j + 4);
									j += 4;
									Integer intField = ByteBuffer.wrap(intFieldB).getInt();
									if (tpos == 0) t.f0 = intField;
									else if (tpos == 1) t.f1 = intField;
									else if (tpos == 2) t.f2 = intField;
								} else if (fieldType.contains("double")) {
									byte[] doubleFieldB = Arrays.copyOfRange(rec, j, j + 8);
									j += 8;
									Double doubleField = ByteBuffer.wrap(doubleFieldB).getDouble();
									if (tpos == 0) t.f0 = doubleField;
									else if (tpos == 1) t.f1 = doubleField;
									else if (tpos == 2) t.f2 = doubleField;
								} else if (fieldType.contains("long")) {
									byte[] longFieldB = Arrays.copyOfRange(rec, j, j + 8);
									j += 8;
									Long longField = ByteBuffer.wrap(longFieldB).getLong();
									if (tpos == 0) t.f0 = longField;
									else if (tpos == 1) t.f1 = longField;
									else if (tpos == 2) t.f2 = longField;
								}
								fieldNum++;
							}
							tpos++;
						}
					}
					records.add((T) t);
					j = 0;
				}
			} else if (type.contains("Tuple4")) {
				int j = 0;

				for (int i = 0; i < b.length; i += recordNumOfBytes) {
					//type, fct, padding, out, actualSize, i);
					Tuple4 t = new Tuple4();
					byte[] rec = this.getTupleByteRecord(type, fc, padding, b, actualSize, i);
					int fieldNum = 0;
					int numOfFields = fc.getFieldTypesRetCopy().size();
					int tpos = 0;
					while (fieldNum < numOfFields) {
						String fieldType = fc.getFieldTypesRetCopy().get(fieldNum);
						if (fc.getNestedTuples()) {
							//TODO
						} else {
							if (fieldType.contains("int")) {
								byte[] intFieldB = Arrays.copyOfRange(rec, j, j + 4);
								j += 4;
								Integer intField = ByteBuffer.wrap(intFieldB).getInt();
								if (tpos == 0) t.f0 = intField;
								else if (tpos == 1) t.f1 = intField;
								else if (tpos == 2) t.f2 = intField;
								else if (tpos == 3) t.f3 = intField;
							} else if (fieldType.contains("double")) {
								byte[] doubleFieldB = Arrays.copyOfRange(rec, j, j + 8);
								j += 8;
								Double doubleField = ByteBuffer.wrap(doubleFieldB).getDouble();
								if (tpos == 0) t.f0 = doubleField;
								else if (tpos == 1) t.f1 = doubleField;
								else if (tpos == 2) t.f2 = doubleField;
								else if (tpos == 3) t.f3 = doubleField;
							} else if (fieldType.contains("long")) {
								byte[] longFieldB = Arrays.copyOfRange(rec, j, j + 8);
								j += 8;
								Long longField = ByteBuffer.wrap(longFieldB).getLong();
								if (tpos == 0) t.f0 = longField;
								else if (tpos == 1) t.f1 = longField;
								else if (tpos == 2) t.f2 = longField;
								else if (tpos == 3) t.f3 = longField;
							}
							fieldNum++;
						}
						tpos++;
					}
					records.add((T) t);
					j = 0;
				}
			}
		}
		return records;
	}

	private static int getArraySizeTuple2 (int arrayFieldNo, String arrayFieldType, Tuple2 t, ArrayList<Integer> actualSizes) {
		int numberOfArrayElements = 0;
		if (arrayFieldNo == 0) {
			if (arrayFieldType.contains("I")) {
				int[] arr = (int[]) t.f0;
				numberOfArrayElements = arr.length;
				actualSizes.add(4);
			} else if (arrayFieldType.contains("D")) {
				double[] arr = (double[]) t.f0;
				actualSizes.add(8);
				numberOfArrayElements = arr.length;
			} else if (arrayFieldType.contains("J")) {
				long[] arr = (long[]) t.f0;
				actualSizes.add(8);
				numberOfArrayElements = arr.length;
			} else if (arrayFieldType.contains("F")) {
				float[] arr = (float[]) t.f0;
				numberOfArrayElements = arr.length;
				actualSizes.add(4);
			}
		} else {
			if (arrayFieldType.contains("I")) {
				int[] arr = (int[]) t.f1;
				numberOfArrayElements = arr.length;
			} else if (arrayFieldType.contains("D")) {
				double[] arr = (double[]) t.f1;
				numberOfArrayElements = arr.length;
			} else if (arrayFieldType.contains("J")) {
				long[] arr = (long[]) t.f1;
				numberOfArrayElements = arr.length;
			} else if (arrayFieldType.contains("F")) {
				float[] arr = (float[]) t.f1;
				numberOfArrayElements = arr.length;
			}
		}
		return numberOfArrayElements;
	}

	private static int getArraySizeTuple3 (int arrayFieldNo, String arrayFieldType, Tuple3 t, ArrayList<Integer> actualSizes) {
		int numberOfArrayElements = 0;
		if (arrayFieldNo == 0) {
			if (arrayFieldType.contains("I")) {
				int[] arr = (int[]) t.f0;
				numberOfArrayElements = arr.length;
				actualSizes.add(4);
			} else if (arrayFieldType.contains("D")) {
				double[] arr = (double[]) t.f0;
				numberOfArrayElements = arr.length;
				actualSizes.add(8);
			} else if (arrayFieldType.contains("J")) {
				long[] arr = (long[]) t.f0;
				numberOfArrayElements = arr.length;
				actualSizes.add(8);
			} else if (arrayFieldType.contains("F")) {
				float[] arr = (float[]) t.f0;
				numberOfArrayElements = arr.length;
				actualSizes.add(4);
			}
		} else if (arrayFieldNo == 1) {
			if (arrayFieldType.contains("I")) {
				int[] arr = (int[]) t.f1;
				numberOfArrayElements = arr.length;
			} else if (arrayFieldType.contains("D")) {
				double[] arr = (double[]) t.f1;
				numberOfArrayElements = arr.length;
			} else if (arrayFieldType.contains("J")) {
				long[] arr = (long[]) t.f1;
				numberOfArrayElements = arr.length;
			} else if (arrayFieldType.contains("F")) {
				float[] arr = (float[]) t.f1;
				numberOfArrayElements = arr.length;
			}
		} else {
			if (arrayFieldType.contains("I")) {
				int[] arr = (int[]) t.f2;
				numberOfArrayElements = arr.length;
			} else if (arrayFieldType.contains("D")) {
				double[] arr = (double[]) t.f2;
				numberOfArrayElements = arr.length;
			} else if (arrayFieldType.contains("J")) {
				long[] arr = (long[]) t.f2;
				numberOfArrayElements = arr.length;
			} else if (arrayFieldType.contains("F")) {
				float[] arr = (float[]) t.f2;
				numberOfArrayElements = arr.length;
			}
		}
		return numberOfArrayElements;
	}

	private static int getArraySizeTuple4 (int arrayFieldNo, String arrayFieldType, Tuple4 t, ArrayList<Integer> actualSizes) {
		int numberOfArrayElements = 0;
		if (arrayFieldNo == 0) {
			if (arrayFieldType.contains("I")) {
				int[] arr = (int[]) t.f0;
				numberOfArrayElements = arr.length;
			} else if (arrayFieldType.contains("D")) {
				double[] arr = (double[]) t.f0;
				numberOfArrayElements = arr.length;
			} else if (arrayFieldType.contains("J")) {
				long[] arr = (long[]) t.f0;
				numberOfArrayElements = arr.length;
			} else if (arrayFieldType.contains("F")) {
				float[] arr = (float[]) t.f0;
				numberOfArrayElements = arr.length;
			}
		} else if (arrayFieldNo == 1) {
			if (arrayFieldType.contains("I")) {
				int[] arr = (int[]) t.f1;
				numberOfArrayElements = arr.length;
			} else if (arrayFieldType.contains("D")) {
				double[] arr = (double[]) t.f1;
				numberOfArrayElements = arr.length;
			} else if (arrayFieldType.contains("J")) {
				long[] arr = (long[]) t.f1;
				numberOfArrayElements = arr.length;
			} else if (arrayFieldType.contains("F")) {
				float[] arr = (float[]) t.f1;
				numberOfArrayElements = arr.length;
			}
		} else if (arrayFieldNo == 2){
			if (arrayFieldType.contains("I")) {
				int[] arr = (int[]) t.f2;
				numberOfArrayElements = arr.length;
			} else if (arrayFieldType.contains("D")) {
				double[] arr = (double[]) t.f2;
				numberOfArrayElements = arr.length;
			} else if (arrayFieldType.contains("J")) {
				long[] arr = (long[]) t.f2;
				numberOfArrayElements = arr.length;
			} else if (arrayFieldType.contains("F")) {
				float[] arr = (float[]) t.f2;
				numberOfArrayElements = arr.length;
			}
		} else {
			if (arrayFieldType.contains("I")) {
				int[] arr = (int[]) t.f3;
				numberOfArrayElements = arr.length;
			} else if (arrayFieldType.contains("D")) {
				double[] arr = (double[]) t.f3;
				numberOfArrayElements = arr.length;
			} else if (arrayFieldType.contains("J")) {
				long[] arr = (long[]) t.f3;
				numberOfArrayElements = arr.length;
			} else if (arrayFieldType.contains("F")) {
				float[] arr = (float[]) t.f3;
				numberOfArrayElements = arr.length;
			}
		}
		return numberOfArrayElements;
	}

	private static int getSizeOfArrayField (String type) {
		int size = 0;
		switch (type) {
			case "class [I":
			case "class [F":
				size = 4;
				break;
			case "class [D":
			case "class [J":
				size = 8;
				break;
		}
		return size;
	}

	// --------------Functions used to get the byte array raw data-------------------
	private static boolean shouldSkipBytes(int counterElements, int counterBlock) {
		boolean condition = (counterElements == 1020);
		if (counterBlock > 2) {
			condition = counterElements == (1024);
		}
		return condition;
	}

	private byte[] getMatrixByteArrayBroadcasted(ArrayList arlist, int bytesToAllocate, int elementTypeSize, TypeInformation type) {
		byte[] bytes = new byte[bytesToAllocate];
		Class<?> typeClass = type.getTypeClass();
		if (typeClass.getName().contains("Float")) {
			int j = 0;
			for (Float[] arr : (ArrayList<Float[]>) arlist) {
				for (float f : arr) {
					int fi = Float.floatToIntBits(f);
					for (int i = 0; i < 4; i++) {
						bytes[j] = (byte) (fi >> (i * 8));
						j++;
					}
				}
			}
		} else if (typeClass.getName().contains("Double")) {
			throw new TornadoFlinkTypeRuntimeException("Broadcasted dataset: Flink-Tornado Data type not supported yet");
		} else if (typeClass.getName().contains("Integer")) {
			throw new TornadoFlinkTypeRuntimeException("Broadcasted dataset: Flink-Tornado Data type not supported yet");
		} else if (typeClass.getName().contains("Long")) {
			throw new TornadoFlinkTypeRuntimeException("Broadcasted dataset: Flink-Tornado Data type not supported yet");
		}
		return bytes;
	}

	private byte[] getMatrixByteArray(int dataTypeSize, byte[] bytes, int bytesToAllocate, int rowBytes, int rowSize, int columnSize) {
		byte[] reversedBytes = new byte[bytesToAllocate];
		int k = 8; // skip first 8 bytes that contain the number of bytes in the row and the number of row elements
		int w = 0;
		for (int i = 0; i < columnSize; i++) {
			for (int j = 0; j < rowSize; j++) {
				k++; // skip element header byte
				for (int z = dataTypeSize - 1; z >= 0; z--) {
					reversedBytes[w + z] = bytes[k];
					k++;
				}
				w += dataTypeSize;
			}
			k += 8;
		}
		return reversedBytes;
	}

	private byte[] getByteArrayBroadcasted(ArrayList arlist, int bytesToAllocate, TypeInformation type) {
		byte[] bytes = new byte[bytesToAllocate];
		Class<?> typeClass = type.getTypeClass();
		if (typeClass.getName().contains("Integer")) {
			int j = 0;
			for (Integer in : (ArrayList<Integer>) arlist) {
				//int fi = Float.floatToIntBits(f);
				//int fib = Integer.
				for (int i = 0; i < 4; i++) {
					bytes[j] = (byte) (in >> (i * 8));
					j++;
				}
			}
		} else if (typeClass.getName().contains("Double")) {
			throw new TornadoFlinkTypeRuntimeException("Broadcasted dataset: Flink-Tornado Data type not supported yet");
		} else if (typeClass.getName().contains("Integer")) {
			throw new TornadoFlinkTypeRuntimeException("Broadcasted dataset: Flink-Tornado Data type not supported yet");
		} else if (typeClass.getName().contains("Long")) {
			throw new TornadoFlinkTypeRuntimeException("Broadcasted dataset: Flink-Tornado Data type not supported yet");
		}
		return bytes;
	}

	private byte[] getByteArray(int dataTypeSize, byte[] bytes, int inputDataSize, int bytesToAllocate) {
		byte[] reversedBytes = new byte[bytesToAllocate];
		int j = (dataTypeSize - 1);
		int numElements = 1;
		for (int i = 4; i < bytes.length; i++) {
			reversedBytes[j] = bytes[i];
			if (j == ((numElements - 1) * dataTypeSize) && numElements < inputDataSize) {
				numElements++;
				// skip header size of record
				i+=4;
				// reset data size
				j = (numElements * dataTypeSize) - 1;
			} else {
				j--;
			}
		}
		return reversedBytes;
	}

	/**
	 * Write bytes of each value reversed (ENDIANESS).
	 */
	private byte[] getByteArrayDataSource(int dataTypeSize, byte[] bytes, int inputDataSize, int bytesToAllocate) {
		byte[] reversedBytes = new byte[bytesToAllocate];
		int j = (dataTypeSize - 1);
		int numElements = 1;
		//final int from = bytesToSkip;
		final int from;
		int headerBytes = 0;
		int counterElements = 0;
		int blocks;
		boolean twoByteHeader = false;
		int extra = 26;
		// the first block can fit up to 1020 bytes
		if (bytesToAllocate <= 1020) {
			headerBytes = 5;
			blocks = 1;
		} else {
			// if the input bytes are over 1020 the blocks are 2 or more
			int bytesInOtherRecords = bytesToAllocate - 1020;
			if (bytesInOtherRecords <= 1024) {
				blocks = 2;
				headerBytes = 5;
				if (bytesInOtherRecords <= 255) {
					headerBytes += 2;
					twoByteHeader = true;
				} else {
					headerBytes += 5;
				}
			} else {
				blocks = 1;
				int bytesLeftToAllocate =  bytesInOtherRecords;
				while (bytesLeftToAllocate > 1024) {
					blocks++;
					bytesLeftToAllocate = bytesLeftToAllocate - 1024;
				}
				if (bytesLeftToAllocate > 0) {
					blocks++;
				}
				if (bytesLeftToAllocate <= 255) {
					headerBytes = (blocks - 1) * 5 + 2;
					twoByteHeader = true;
				} else {
					headerBytes = (blocks) * 5;
				}
			}

		}
		from = bytes.length - ((bytesToAllocate + (headerBytes - 5) + 1 + extra));
		final int to = bytes.length - 1 - extra;
		int counterBlock = 2;
		for (int i = from; i < to; i++) {
			//System.out.println("counterElements: " + counterElements + " counterBlock: " + counterBlock);
			if (shouldSkipBytes(counterElements, counterBlock)) {
				i += 2;
				if (counterBlock <= blocks ) {
					if (!((counterBlock == blocks) && twoByteHeader)) {
						i += 3;
					}
				}
				counterElements = 0;
				counterBlock++;
			}

			reversedBytes[j] = bytes[i];
			counterElements++;

			if (j == ((numElements - 1) * dataTypeSize) && numElements < inputDataSize) {
				numElements++;
				// reset data size
				j = (numElements * dataTypeSize) - 1;
			} else {
				j--;
			}
		}
		return reversedBytes;
	}

	private byte[] getTupleByteArrayBroadcasted(int numberOfElements, ArrayList ar, int bytesToAllocate, TypeInformation[] tuparray, int arrayFieldNo,int numberOfArrayElements,int arrayElementSize, boolean padding) {
		byte[] reversedBytes = new byte[bytesToAllocate];
		int tupleSize = tuparray.length;
		int numberOfTuples = ar.size();
		int i = 0;
		if (tupleSize == 2) {
			for (int t = 0; t < numberOfTuples; t++) {
				Tuple2 t2 = (Tuple2) ar.get(t);
				// Field 0
				if (tuparray[0].getTypeClass().toString().contains("Double")) {

				} else if (tuparray[0].getTypeClass().toString().contains("[I")) {
				} else if (tuparray[0].getTypeClass().toString().contains("[D")) {
					double[] arrayField = (double[]) t2.f0;
					for (int j = 0; j < arrayField.length; j++) {
						byte[] byteField = new byte[8];
						ByteBuffer.wrap(byteField).putDouble(arrayField[j]);
						changeOutputEndianess8(byteField);
						for (int k = 0; k < 8; k++) {
							reversedBytes[i] = byteField[k];
							i++;
						}
					}
				} else if (tuparray[0].getTypeClass().toString().contains("[F")) {
					float[] arrayField = (float[]) t2.f0;
					for (int j = 0; j < arrayField.length; j++) {
						byte[] byteField = new byte[4];
						ByteBuffer.wrap(byteField).putFloat(arrayField[j]);
						changeOutputEndianess4(byteField);
						for (int k = 0; k < 4; k++) {
							reversedBytes[i] = byteField[k];
							i++;
						}
					}
				} else if (tuparray[0].getTypeClass().toString().contains("[J")) {

				} else if (tuparray[0].getTypeClass().toString().contains("Float")) {
					Float f = (Float) t2.f0;
					int fi = Float.floatToIntBits(f);
					for (int k = 0; k < 4; k++) {
						reversedBytes[i] = (byte) ((fi) >> (k * 8));
						i++;
					}
				} else if (tuparray[0].getTypeClass().toString().contains("Long")) {

				} else if (tuparray[0].getTypeClass().toString().contains("Integer")) {

				}
				// Field 1
				if (tuparray[1].getTypeClass().toString().contains("Double")) {

				} else if (tuparray[1].getTypeClass().toString().contains("[I")) {

				} else if (tuparray[1].getTypeClass().toString().contains("[D")) {
					Double[] arrayField = (Double[]) t2.f1;
					for (int j = 0; j < arrayField.length; j++) {
						byte[] byteField = new byte[8];
						ByteBuffer.wrap(byteField).putDouble(arrayField[j]);
						changeOutputEndianess8(byteField);
						for (int k = 0; k < 8; k++) {
							reversedBytes[i] = byteField[k];
							i++;
						}
					}
				} else if (tuparray[1].getTypeClass().toString().contains("[F")) {
				} else if (tuparray[1].getTypeClass().toString().contains("[J")) {

				} else if (tuparray[1].getTypeClass().toString().contains("Float")) {
					Float f = (Float) t2.f1;
					int fi = Float.floatToIntBits(f);
					for (int k = 0; k < 4; k++) {
						reversedBytes[i] = (byte) ((fi) >> (k * 8));
						i++;
					}

				} else if (tuparray[1].getTypeClass().toString().contains("Long")) {

				} else if (tuparray[1].getTypeClass().toString().contains("Integer")) {
					Integer in = (Integer) t2.f1;
					byte[] byteField = new byte[4];
					ByteBuffer.wrap(byteField).putInt(in);
					changeOutputEndianess4(byteField);
					for (int k = 0; k < 4; k++) {
						reversedBytes[i] = byteField[k];
						i++;
					}
				}
			}
		} else if (tupleSize == 3) {
			for (int t = 0; t < numberOfTuples; t++) {
				Tuple3 t3 = (Tuple3) ar.get(t);
				// Field 0
				if (tuparray[0].getTypeClass().toString().contains("Double")) {

				} else if (tuparray[0].getTypeClass().toString().contains("[I")) {
				} else if (tuparray[0].getTypeClass().toString().contains("[D")) {
					double[] arrayField = (double[]) t3.f0;
					for (int j = 0; j < arrayField.length; j++) {
						byte[] byteField = new byte[8];
						ByteBuffer.wrap(byteField).putDouble(arrayField[j]);
						changeOutputEndianess8(byteField);
						for (int k = 0; k < 8; k++) {
							reversedBytes[i] = byteField[k];
							i++;
						}
					}
				} else if (tuparray[0].getTypeClass().toString().contains("[F")) {
					float[] arrayField = (float[]) t3.f0;
					for (int j = 0; j < arrayField.length; j++) {
						byte[] byteField = new byte[4];
						ByteBuffer.wrap(byteField).putFloat(arrayField[j]);
						changeOutputEndianess4(byteField);
						for (int k = 0; k < 4; k++) {
							reversedBytes[i] = byteField[k];
							i++;
						}
					}
				} else if (tuparray[0].getTypeClass().toString().contains("[J")) {

				} else if (tuparray[0].getTypeClass().toString().contains("Float")) {

				} else if (tuparray[0].getTypeClass().toString().contains("Long")) {

				} else if (tuparray[0].getTypeClass().toString().contains("Integer")) {
					Integer in = (Integer) t3.f0;
					byte[] byteField = new byte[4];
					ByteBuffer.wrap(byteField).putInt(in);
					changeOutputEndianess4(byteField);
					for (int k = 0; k < 4; k++) {
						reversedBytes[i] = byteField[k];
						i++;
					}
					i+=4;
				}
				// Field 1
				if (tuparray[1].getTypeClass().toString().contains("Double")) {
					Double d = (Double) t3.f1;
					byte[] byteField = new byte[8];
					ByteBuffer.wrap(byteField).putDouble(d);
					changeOutputEndianess8(byteField);
					for (int k = 0; k < 8; k++) {
						reversedBytes[i] = byteField[k];
						i++;
					}
				} else if (tuparray[1].getTypeClass().toString().contains("[I")) {

				} else if (tuparray[1].getTypeClass().toString().contains("[D")) {
					Double[] arrayField = (Double[]) t3.f1;
					for (int j = 0; j < arrayField.length; j++) {
						byte[] byteField = new byte[8];
						ByteBuffer.wrap(byteField).putDouble(arrayField[j]);
						changeOutputEndianess8(byteField);
						for (int k = 0; k < 8; k++) {
							reversedBytes[i] = byteField[k];
							i++;
						}
					}
				} else if (tuparray[1].getTypeClass().toString().contains("[F")) {
				} else if (tuparray[1].getTypeClass().toString().contains("[J")) {

				} else if (tuparray[1].getTypeClass().toString().contains("Float")) {

				} else if (tuparray[1].getTypeClass().toString().contains("Long")) {

				} else if (tuparray[1].getTypeClass().toString().contains("Integer")) {
					Integer in = (Integer) t3.f1;
					byte[] byteField = new byte[4];
					ByteBuffer.wrap(byteField).putInt(in);
					changeOutputEndianess4(byteField);
					for (int k = 0; k < 4; k++) {
						reversedBytes[i] = byteField[k];
						i++;
					}
				} else if (tuparray[1].getTypeClass().toString().contains("Tuple2")) {
					Tuple2 tuple2 = (Tuple2) t3.f1;
					if (tuple2.f0.getClass().toString().contains("Double")) {
						Double d = (Double) tuple2.f0;
						byte[] byteField = new byte[8];
						ByteBuffer.wrap(byteField).putDouble(d);
						changeOutputEndianess8(byteField);
						for (int k = 0; k < 8; k++) {
							reversedBytes[i] = byteField[k];
							i++;
						}
					}
					if (tuple2.f1.getClass().toString().contains("Double")) {
						Double d = (Double) tuple2.f1;
						byte[] byteField = new byte[8];
						ByteBuffer.wrap(byteField).putDouble(d);
						changeOutputEndianess8(byteField);
						for (int k = 0; k < 8; k++) {
							reversedBytes[i] = byteField[k];
							i++;
						}
					}
				}

				// Field 3
				if (tuparray[2].getTypeClass().toString().contains("Double")) {
					Double d = (Double) t3.f2;
					byte[] byteField = new byte[8];
					ByteBuffer.wrap(byteField).putDouble(d);
					changeOutputEndianess8(byteField);
					for (int k = 0; k < 8; k++) {
						reversedBytes[i] = byteField[k];
						i++;
					}
				} else if (tuparray[2].getTypeClass().toString().contains("[I")) {

				} else if (tuparray[2].getTypeClass().toString().contains("[D")) {
					Double[] arrayField = (Double[]) t3.f2;
					for (int j = 0; j < arrayField.length; j++) {
						byte[] byteField = new byte[8];
						ByteBuffer.wrap(byteField).putDouble(arrayField[j]);
						changeOutputEndianess8(byteField);
						for (int k = 0; k < 8; k++) {
							reversedBytes[i] = byteField[k];
							i++;
						}
					}
				} else if (tuparray[2].getTypeClass().toString().contains("[F")) {
				} else if (tuparray[2].getTypeClass().toString().contains("[J")) {

				} else if (tuparray[2].getTypeClass().toString().contains("Float")) {

				} else if (tuparray[2].getTypeClass().toString().contains("Long")) {
					Long d = (Long) t3.f2;
					byte[] byteField = new byte[8];
					ByteBuffer.wrap(byteField).putLong(d);
					changeOutputEndianess8(byteField);
					for (int k = 0; k < 8; k++) {
						reversedBytes[i] = byteField[k];
						i++;
					}
				} else if (tuparray[2].getTypeClass().toString().contains("Integer")) {
					Integer in = (Integer) t3.f2;
					byte[] byteField = new byte[4];
					ByteBuffer.wrap(byteField).putInt(in);
					changeOutputEndianess4(byteField);
					for (int k = 0; k < 4; k++) {
						reversedBytes[i] = byteField[k];
						i++;
					}
				}
			}
		} else if (tupleSize == 4) {

		} else {
			System.out.println("WE CURRENTLY SUPPORT UP TO TUPLE4");
		}

		return reversedBytes;
	}

	private byte[] getTupleByteArray(int numberOfTuples, byte[] bytes, int bytesToAllocate, int[] tupleFieldsSizes, int fieldWithArray, int fieldArraySize, int fieldArrayElementSize, boolean padding, boolean header) {
		byte[] reversedBytes = new byte[bytesToAllocate];
		int totalBytesWritten = 0;
		int next = 0;
		int j = 0;
		for (int t = 0; t < numberOfTuples; t++) {
			//for every field of each tuple
			for (int s = 0; s < tupleFieldsSizes.length; s++) {
				if (s != fieldWithArray) {
					// in the first field skip first 4 bytes for record size
					if (header) {
						if (s == 0) {
							j += 4;
						}
					}
					next = totalBytesWritten;
					//for every byte of each field
					int fieldSize = tupleFieldsSizes[s];
					//System.out.println("Tuple number: " + t + " , field " + s + " , fieldSize = " + fieldSize);
					for (int i = tupleFieldsSizes[s] - 1; i >= 0; i--) {
						//System.out.println("** write at pos " + (i + next) + " byte[" + j + "]");
						reversedBytes[i + next] = bytes[j];
						//System.out.println("** reversedBytes[" + (i + next) + "] = " + reversedBytes[i + next]);
						j++;
						totalBytesWritten++;
						if (padding && (fieldSize == 4) && (i == 0)) {
							for (int k = 0; k < 4; k++) {
								//System.out.println("++ write 0 at pos " + (4 + next + k));
								reversedBytes[4 + next + k] = 0;
								//System.out.println("++ reversedBytes[" + (4 + next + k) + "] = " + reversedBytes[4 + next + k]);
								totalBytesWritten++;
							}
						}
					}
				} else {
					next = totalBytesWritten;
					//for every byte of each field
					// first skip 8 bytes for the size of the array and the record size if array is first field
					// else skip 4 bytes for the array header if array
					if (header) {
						if (s == 0) {
							j += 8;
						} else {
							j += 4;
						}
					}
					int pad = 0;
					for (int k = 0; k < fieldArraySize; k++) {
						for (int i = fieldArrayElementSize - 1; i >= 0; i--) {
							reversedBytes[i + next + k * fieldArrayElementSize + pad] = bytes[j];
							j++;
							totalBytesWritten++;
							// add padding in array elements
							if (padding && (fieldArrayElementSize == 4) && (i == 0)) {
								for (int w = 0; w < 4; w++) {
									reversedBytes[4 + next + w + 8 * k] = 0;
									totalBytesWritten++;
								}
								pad = 4 * (k + 1);
							}
						}
					}
				}
			}
		}
		return reversedBytes;
	}

		/**
		 * Write bytes of each field of a tuple reversed (ENDIANESS).
		 */
	private byte[] getTupleByteArrayDataSource(int numberOftuples, byte[] bytes, int bytesToAllocate, int rawBytes, int[] tupleFieldsSizes, int fieldWithArray, int fieldArraySize, int fieldArrayElementSize, boolean padding) {
		byte[] reversedBytes = new byte[bytesToAllocate];
		// when calculating the number of blocks take into account the 4 array header bytes, if one tuple field is an array
		if (fieldWithArray != -1) {
			rawBytes = rawBytes + 4 * numberOftuples;
		}
		int j = tupleFieldsSizes[0];
		final int from;
		int headerBytes = 0;
		int blocks;
		boolean twoByteHeader = false;
		int extra = 26;
		// the first block can fit up to 1020 bytes
		if (rawBytes <= 1020) {
			headerBytes = 5;
			blocks = 1;
		} else {
			// if the input bytes are over 1020 the blocks are 2 or more
			int bytesInOtherRecords = rawBytes - 1020;
			if (bytesInOtherRecords <= 1024) {
				blocks = 2;
				headerBytes = 5;
				if (bytesInOtherRecords <= 255) {
					headerBytes += 2;
					twoByteHeader = true;
				} else {
					headerBytes += 5;
				}
			} else {
				blocks = 1;
				int bytesLeftToAllocate =  bytesInOtherRecords;
				while (bytesLeftToAllocate > 1024) {
					blocks++;
					bytesLeftToAllocate = bytesLeftToAllocate - 1024;
				}
				if (bytesLeftToAllocate > 0) {
					blocks++;
				}
				if (bytesLeftToAllocate <= 255) {
					headerBytes = (blocks - 1) * 5 + 2;
					twoByteHeader = true;
				} else {
					headerBytes = (blocks) * 5;
				}
			}

		}

		if (fieldWithArray != -1) {
			rawBytes = rawBytes - numberOftuples * 4;
		}

		if (fieldWithArray == -1) {
			from = bytes.length - ((rawBytes + (headerBytes - 5) + 1 + extra));
		} else {
			from = bytes.length - ((rawBytes + (headerBytes - 5) + 1  + extra + 4 * numberOftuples));
		}
		int counterBlock = 2;
		j = from;
		int bytesWrittenBlock = 0;
		int totalBytesWritten = 0;
		int next;
		//for every tuple
		for (int t = 0; t < numberOftuples; t++) {
			//for every field of each tuple
			for (int s = 0; s < tupleFieldsSizes.length; s++) {
				if (s != fieldWithArray) {
					next = totalBytesWritten;
					//for every byte of each field
					int fieldSize = tupleFieldsSizes[s];
					//System.out.println("Tuple number: " + t + " , field " + s + " , fieldSize = " + fieldSize);
					for (int i = tupleFieldsSizes[s] - 1; i >= 0; i--) {
						if (shouldSkipBytes(bytesWrittenBlock, counterBlock)) {
							j += 2;
							if (counterBlock <= blocks) {
								if (!((counterBlock == blocks) && twoByteHeader)) {
									j += 3;
								}
							}
							bytesWrittenBlock = 0;
							counterBlock++;
						}
						//System.out.println("** write at pos " + (i + next) + " byte[" + j + "]");
						reversedBytes[i + next] = bytes[j];
						//System.out.println("** reversedBytes[" + (i + next) + "] = " + reversedBytes[i + next]);
						j++;
						totalBytesWritten++;
						bytesWrittenBlock++;
						if (padding && (fieldSize == 4) && (i == 0)) {
							for (int k = 0; k < 4; k++) {
								//System.out.println("++ write 0 at pos " + (4 + next + k));
								reversedBytes[4 + next + k] = 0;
								//System.out.println("++ reversedBytes[" + (4 + next + k) + "] = " + reversedBytes[4 + next + k]);
								totalBytesWritten++;
							}
						}
					}
				} else {
					next = totalBytesWritten;
					//for every byte of each field
					// first skip 4 bytes for the size of the array
					for (int z = 0; z < 4; z++) {
						if (shouldSkipBytes(bytesWrittenBlock, counterBlock)) {
							j += 2;
							if (counterBlock <= blocks) {
								if (!((counterBlock == blocks) && twoByteHeader)) {
									j += 3;
								}
							}
							bytesWrittenBlock = 0;
							counterBlock++;
						}
						j++;
						bytesWrittenBlock++;
					}
					int pad = 0;
					for (int k = 0; k < fieldArraySize; k++) {
						for (int i = fieldArrayElementSize - 1; i >= 0; i--) {
							if (shouldSkipBytes(bytesWrittenBlock, counterBlock)) {
								j += 2;
								if (counterBlock <= blocks) {
									if (!((counterBlock == blocks) && twoByteHeader)) {
										j += 3;
									}
								}
								bytesWrittenBlock = 0;
								counterBlock++;
							}

							reversedBytes[i + next + k * fieldArrayElementSize + pad] = bytes[j];
							j++;
							totalBytesWritten++;
							bytesWrittenBlock++;
							// add padding in array elements
							if (padding && (fieldArrayElementSize == 4) && (i == 0)) {
								for (int w = 0; w < 4; w++) {
									reversedBytes[4 + next + w + 8 * k] = 0;
									totalBytesWritten++;
								}
								pad = 4 * (k + 1);
							}
						}
					}
				}
			}
		}
		return reversedBytes;
	}

	public byte[] getMatrixRowBytes (byte[] out, int numOfBytesInRow, int numOfColumns,int recordSize) {
		byte[] recordBytes = new byte[numOfBytesInRow];
		byte[] numOfBytes = new byte[4];
		byte[] numOfCol = new byte[4];
		ByteBuffer.wrap(numOfBytes).putInt(numOfBytesInRow);
		ByteBuffer.wrap(numOfCol).putInt(numOfColumns);
		for (int i = 0; i < 4; i++) {
			//recordBytes[i] = numOfBytes[i];
			//recordBytes[i + 4] = numOfCol[i];
			recordBytes[i] = numOfCol[i];
		}
		int typeSize = recordSize / numOfColumns;
		for (int i = 0; i < numOfColumns; i++) {
			recordBytes[4 + i * 4 + i] = 1;
			if (typeSize == 4) {
				for (int j = 0; j < 4; j++) {
					recordBytes[(4 + i*4 + j + i + 1)] = out[(i*4 + j)];
				}
			} else if (typeSize == 8) {
				for (int j = 0; j < 8; j++) {
					recordBytes[(4 + i*8 + j + 1)] = out[(i*8 + (7 - j))];
				}
			}
		}
		return recordBytes;
	}

	public byte[] getInputTupleByteRecord (String type, FlinkCompilerInfo fct, boolean padding, byte[] out, int actualSize, int i) {
		byte[] recordBytes = null;
		if (fct.getFieldSizes().size() ==2) {
			int destPos = 0;
			for (int j = 0; j < fct.getFieldSizes().size(); j++) {
				int fieldSize = fct.getFieldSizes().get(j);
				byte[] f = null;
				if (padding) {
					if (fieldSize == 8) {
						if (j == 0) {
							f = Arrays.copyOfRange(out, i, (i + 8));
						} else {
							f = Arrays.copyOfRange(out, (i + 8), (i + 8 + 8));
						}
						changeOutputEndianess8(f);
					} else if (fieldSize == 4) {
						if (j == 0) {
							f = Arrays.copyOfRange(out, i, (4 + i));
						} else {
							f = Arrays.copyOfRange(out, (i + 8), (4 + i + 8));
						}
						changeOutputEndianess4(f);
					}
				} else {
					if (fieldSize == 8) {
						if (j == 0) {
							f = Arrays.copyOfRange(out, i, (i + 8));
						} else {
							f = Arrays.copyOfRange(out, (i + fct.getFieldSizes().get(0)), (i + 8 + fct.getFieldSizes().get(0)));
						}
						changeOutputEndianess8(f);
					} else if (fieldSize == 4) {
						if (j == 0) {
							f = Arrays.copyOfRange(out, i, (4 + i));
						} else {
							f = Arrays.copyOfRange(out, (i + fct.getFieldSizes().get(0)), (4 + i + fct.getFieldSizes().get(0)));
						}
						changeOutputEndianess4(f);
					}
				}

				if (recordBytes == null) {
					// write first array
					recordBytes = Arrays.copyOf(f, actualSize);
					destPos += f.length;
				} else {
					System.arraycopy(f, 0, recordBytes, destPos, f.length);
					destPos += f.length;
				}
			}
		} else if (fct.getFieldSizes().size() == 3) {
			int destPos = 0;
			for (int j = 0; j < fct.getFieldSizes().size(); j++) {
				int fieldSize = fct.getFieldSizes().get(j);
				byte[] f = null;
				if (padding) {
					if (fieldSize == 8) {
						if (j == 0) {
							f = Arrays.copyOfRange(out, i, (i + 8));
						} else if (j == 1) {
							f = Arrays.copyOfRange(out, (i + 8), (i + 8 + 8));
						} else {
							f = Arrays.copyOfRange(out, (i + 8 + 8), (i + 8 + 8 + 8));
						}
						changeOutputEndianess8(f);
					} else if (fieldSize == 4) {
						if (j == 0) {
							f = Arrays.copyOfRange(out, i, (4 + i));
						} else if (j == 1) {
							f = Arrays.copyOfRange(out, (i + 8), (4 + i + 8));
						} else {
							f = Arrays.copyOfRange(out, (i + 8 + 8), (i + 4 + 8 + 8));
						}
						changeOutputEndianess4(f);
					}
				} else {
					if (fieldSize == 8) {
						if (j == 0) {
							f = Arrays.copyOfRange(out, i, (i + 8));
						} else if (j == 1) {
							f = Arrays.copyOfRange(out, (i + fct.getFieldSizes().get(0)), (i + 8 + fct.getFieldSizes().get(0)));
						} else {
							f = Arrays.copyOfRange(out, (i + fct.getFieldSizes().get(0) + fct.getFieldSizes().get(1)), (i + 8 + fct.getFieldSizes().get(0) + fct.getFieldSizes().get(1)));
						}
						changeOutputEndianess8(f);
					} else if (fieldSize == 4) {
						if (j == 0) {
							f = Arrays.copyOfRange(out, i, (4 + i));
						} else if (j == 1) {
							f = Arrays.copyOfRange(out, (i + fct.getFieldSizes().get(0)), (4 + i + fct.getFieldSizes().get(0)));
						} else {
							f = Arrays.copyOfRange(out, (i + fct.getFieldSizes().get(0) + fct.getFieldSizes().get(1)), (i + 4 + fct.getFieldSizes().get(0) + fct.getFieldSizes().get(1)));
						}
						changeOutputEndianess4(f);
					}
				}

				if (recordBytes == null) {
					// write first array
					recordBytes = Arrays.copyOf(f, actualSize);
					destPos += f.length;
				} else {
					System.arraycopy(f, 0, recordBytes, destPos, f.length);
					destPos += f.length;
				}
			}
		} else if (fct.getFieldSizes().size() == 4) {
			int destPos = 0;
			for (int j = 0; j < fct.getFieldSizes().size(); j++) {
				int fieldSize = fct.getFieldSizes().get(j);
				byte[] f = null;
				if (padding) {
					if (fieldSize == 8) {
						if (j == 0) {
							f = Arrays.copyOfRange(out, i, (i + 8));
						} else if (j == 1) {
							f = Arrays.copyOfRange(out, (i + 8), (i + 8 + 8));
						} else if (j == 2) {
							f = Arrays.copyOfRange(out, (i + 8 + 8), (i + 8 + 8 + 8));
						} else {
							f = Arrays.copyOfRange(out, (i + 8 + 8 + 8), (i + 8 + 8 + 8 + 8));
						}
						changeOutputEndianess8(f);
					} else if (fieldSize == 4) {
						if (j == 0) {
							f = Arrays.copyOfRange(out, i, (4 + i));
						} else if (j == 1) {
							f = Arrays.copyOfRange(out, (i + 8), (4 + i + 8));
						} else if (j == 2) {
							f = Arrays.copyOfRange(out, (i + 8 + 8), (i + 4 + 8 + 8));
						} else {
							f = Arrays.copyOfRange(out, (i + 8 + 8 + 8), (i + 4 + 8 + 8 + 8));
						}
						changeOutputEndianess4(f);
					}
				} else {
					if (fieldSize == 8) {
						if (j == 0) {
							f = Arrays.copyOfRange(out, i, (i + 8));
						} else if (j == 1) {
							f = Arrays.copyOfRange(out, (i + fct.getFieldSizes().get(0)), (i + 8 + fct.getFieldSizes().get(0)));
						} else if (j == 2) {
							f = Arrays.copyOfRange(out, (i + fct.getFieldSizes().get(0) + fct.getFieldSizes().get(1)), (i + 8 + fct.getFieldSizes().get(0) + fct.getFieldSizes().get(1)));
						} else {
							f = Arrays.copyOfRange(out, (i + fct.getFieldSizes().get(0) + fct.getFieldSizes().get(1) + fct.getFieldSizes().get(2)), (i + 8 + fct.getFieldSizes().get(0) + fct.getFieldSizes().get(1) + fct.getFieldSizes().get(2)));
						}
						changeOutputEndianess8(f);
					} else if (fieldSize == 4) {
						if (j == 0) {
							f = Arrays.copyOfRange(out, i, (4 + i));
						} else if (j == 1) {
							f = Arrays.copyOfRange(out, (i + fct.getFieldSizes().get(0)), (4 + i + fct.getFieldSizes().get(0)));
						} else if (j == 2) {
							f = Arrays.copyOfRange(out, (i + fct.getFieldSizes().get(0) + fct.getFieldSizes().get(1)), (i + 4 + fct.getFieldSizes().get(0) + fct.getFieldSizes().get(1)));
						} else {
							f = Arrays.copyOfRange(out, (i + fct.getFieldSizes().get(0) + fct.getFieldSizes().get(1) + fct.getFieldSizes().get(2)), (i + 4 + fct.getFieldSizes().get(0) + fct.getFieldSizes().get(1) + fct.getFieldSizes().get(2)));
						}
						changeOutputEndianess4(f);
					}
				}

				if (recordBytes == null) {
					// write first array
					recordBytes = Arrays.copyOf(f, actualSize);
					destPos += f.length;
				} else {
					System.arraycopy(f, 0, recordBytes, destPos, f.length);
					destPos += f.length;
				}
			}
		}
		return recordBytes;
	}

	public byte[] getTupleByteRecord (String type, FlinkCompilerInfo fct, boolean padding, byte[] out, int actualSize, int i) {
		byte[] recordBytes = null;
		if (fct.getFieldSizesRet().size() ==2) {
			int destPos = 0;
			for (int j = 0; j < fct.getFieldSizesRet().size(); j++) {
				int fieldSize = fct.getFieldSizesRet().get(j);
				byte[] f = null;
				if (padding) {
					if (fieldSize == 8) {
						if (j == 0) {
							f = Arrays.copyOfRange(out, i, (i + 8));
						} else {
							f = Arrays.copyOfRange(out, (i + 8), (i + 8 + 8));
						}
						changeOutputEndianess8(f);
					} else if (fieldSize == 4) {
						if (j == 0) {
							f = Arrays.copyOfRange(out, i, (4 + i));
						} else {
							f = Arrays.copyOfRange(out, (i + 8), (4 + i + 8));
						}
						changeOutputEndianess4(f);
					}
				} else {
					if (fieldSize == 8) {
						if (j == 0) {
							f = Arrays.copyOfRange(out, i, (i + 8));
						} else {
							f = Arrays.copyOfRange(out, (i + fct.getFieldSizesRet().get(0)), (i + 8 + fct.getFieldSizesRet().get(0)));
						}
						changeOutputEndianess8(f);
					} else if (fieldSize == 4) {
						if (j == 0) {
							f = Arrays.copyOfRange(out, i, (4 + i));
						} else {
							f = Arrays.copyOfRange(out, (i + fct.getFieldSizesRet().get(0)), (4 + i + fct.getFieldSizesRet().get(0)));
						}
						changeOutputEndianess4(f);
					}
				}

				if (recordBytes == null) {
					// write first array
					recordBytes = Arrays.copyOf(f, actualSize);
					destPos += f.length;
				} else {
					System.arraycopy(f, 0, recordBytes, destPos, f.length);
					destPos += f.length;
				}
			}
		} else if (fct.getFieldSizesRet().size() == 3) {
			int destPos = 0;
			for (int j = 0; j < fct.getFieldSizesRet().size(); j++) {
				int fieldSize = fct.getFieldSizesRet().get(j);
				byte[] f = null;
				if (padding) {
					if (fieldSize == 8) {
						if (j == 0) {
							f = Arrays.copyOfRange(out, i, (i + 8));
						} else if (j == 1) {
							f = Arrays.copyOfRange(out, (i + 8), (i + 8 + 8));
						} else {
							f = Arrays.copyOfRange(out, (i + 8 + 8), (i + 8 + 8 + 8));
						}
						changeOutputEndianess8(f);
					} else if (fieldSize == 4) {
						if (j == 0) {
							f = Arrays.copyOfRange(out, i, (4 + i));
						} else if (j == 1) {
							f = Arrays.copyOfRange(out, (i + 8), (4 + i + 8));
						} else {
							f = Arrays.copyOfRange(out, (i + 8 + 8), (i + 4 + 8 + 8));
						}
						changeOutputEndianess4(f);
					}
				} else {
					if (fieldSize == 8) {
						if (j == 0) {
							f = Arrays.copyOfRange(out, i, (i + 8));
						} else if (j == 1) {
							f = Arrays.copyOfRange(out, (i + fct.getFieldSizesRet().get(0)), (i + 8 + fct.getFieldSizesRet().get(0)));
						} else {
							f = Arrays.copyOfRange(out, (i + fct.getFieldSizesRet().get(0) + fct.getFieldSizesRet().get(1)), (i + 8 + fct.getFieldSizesRet().get(0) + fct.getFieldSizesRet().get(1)));
						}
						changeOutputEndianess8(f);
					} else if (fieldSize == 4) {
						if (j == 0) {
							f = Arrays.copyOfRange(out, i, (4 + i));
						} else if (j == 1) {
							f = Arrays.copyOfRange(out, (i + fct.getFieldSizesRet().get(0)), (4 + i + fct.getFieldSizesRet().get(0)));
						} else {
							f = Arrays.copyOfRange(out, (i + fct.getFieldSizesRet().get(0) + fct.getFieldSizesRet().get(1)), (i + 4 + fct.getFieldSizesRet().get(0) + fct.getFieldSizesRet().get(1)));
						}
						changeOutputEndianess4(f);
					}
				}

				if (recordBytes == null) {
					// write first array
					recordBytes = Arrays.copyOf(f, actualSize);
					destPos += f.length;
				} else {
					System.arraycopy(f, 0, recordBytes, destPos, f.length);
					destPos += f.length;
				}
			}
		} else if (fct.getFieldSizesRet().size() == 4) {
			int destPos = 0;
			for (int j = 0; j < fct.getFieldSizesRet().size(); j++) {
				int fieldSize = fct.getFieldSizesRet().get(j);
				byte[] f = null;
				if (padding) {
					if (fieldSize == 8) {
						if (j == 0) {
							f = Arrays.copyOfRange(out, i, (i + 8));
						} else if (j == 1) {
							f = Arrays.copyOfRange(out, (i + 8), (i + 8 + 8));
						} else if (j == 2) {
							f = Arrays.copyOfRange(out, (i + 8 + 8), (i + 8 + 8 + 8));
						} else {
							f = Arrays.copyOfRange(out, (i + 8 + 8 + 8), (i + 8 + 8 + 8 + 8));
						}
						changeOutputEndianess8(f);
					} else if (fieldSize == 4) {
						if (j == 0) {
							f = Arrays.copyOfRange(out, i, (4 + i));
						} else if (j == 1) {
							f = Arrays.copyOfRange(out, (i + 8), (4 + i + 8));
						} else if (j == 2) {
							f = Arrays.copyOfRange(out, (i + 8 + 8), (i + 4 + 8 + 8));
						} else {
							f = Arrays.copyOfRange(out, (i + 8 + 8 + 8), (i + 4 + 8 + 8 + 8));
						}
						changeOutputEndianess4(f);
					}
				} else {
					if (fieldSize == 8) {
						if (j == 0) {
							f = Arrays.copyOfRange(out, i, (i + 8));
						} else if (j == 1) {
							f = Arrays.copyOfRange(out, (i + fct.getFieldSizesRet().get(0)), (i + 8 + fct.getFieldSizesRet().get(0)));
						} else if (j == 2) {
							f = Arrays.copyOfRange(out, (i + fct.getFieldSizesRet().get(0) + fct.getFieldSizesRet().get(1)), (i + 8 + fct.getFieldSizesRet().get(0) + fct.getFieldSizesRet().get(1)));
						} else {
							f = Arrays.copyOfRange(out, (i + fct.getFieldSizesRet().get(0) + fct.getFieldSizesRet().get(1) + fct.getFieldSizesRet().get(2)), (i + 8 + fct.getFieldSizesRet().get(0) + fct.getFieldSizesRet().get(1) + fct.getFieldSizesRet().get(2)));
						}
						changeOutputEndianess8(f);
					} else if (fieldSize == 4) {
						if (j == 0) {
							f = Arrays.copyOfRange(out, i, (4 + i));
						} else if (j == 1) {
							f = Arrays.copyOfRange(out, (i + fct.getFieldSizesRet().get(0)), (4 + i + fct.getFieldSizesRet().get(0)));
						} else if (j == 2) {
							f = Arrays.copyOfRange(out, (i + fct.getFieldSizesRet().get(0) + fct.getFieldSizesRet().get(1)), (i + 4 + fct.getFieldSizesRet().get(0) + fct.getFieldSizesRet().get(1)));
						} else {
							f = Arrays.copyOfRange(out, (i + fct.getFieldSizesRet().get(0) + fct.getFieldSizesRet().get(1) + fct.getFieldSizesRet().get(2)), (i + 4 + fct.getFieldSizesRet().get(0) + fct.getFieldSizesRet().get(1) + fct.getFieldSizesRet().get(2)));
						}
						changeOutputEndianess4(f);
					}
				}

				if (recordBytes == null) {
					// write first array
					recordBytes = Arrays.copyOf(f, actualSize);
					destPos += f.length;
				} else {
					System.arraycopy(f, 0, recordBytes, destPos, f.length);
					destPos += f.length;
				}
			}
		}
		return recordBytes;
	}

	public static byte[] changeOutputEndianess4(byte[] output) {
		byte tmp;
		for (int i = 0; i < output.length; i += 4) {
			// swap 0 and 3
			tmp = output[i];
			output[i] = output[i + 3];
			output[i + 3] = tmp;
			// swap 1 and 2
			tmp = output[i + 1];
			output[i + 1] = output[i + 2];
			output[i + 2] = tmp;
		}
		return output;
	}

	public static byte[] changeOutputEndianess8(byte[] output) {
		byte tmp;
		for (int i = 0; i < output.length; i += 8) {
			// swap 0 and 7
			tmp = output[i];
			output[i] = output[i + 7];
			output[i + 7] = tmp;
			// swap 1 and 6
			tmp = output[i + 1];
			output[i + 1] = output[i + 6];
			output[i + 6] = tmp;
			// swap 2 and 5
			tmp = output[i + 2];
			output[i + 2] = output[i + 5];
			output[i + 5] = tmp;
			// swap 3 and 4
			tmp = output[i + 3];
			output[i + 3] = output[i + 4];
			output[i + 4] = tmp;
		}
		return output;
	}
	/**
	 * Function the returns the number of bytes of type.
	 */
	private static int getTypeBytesNumericType(TypeInformation type) {
		if (type.isBasicType() || type instanceof NumericTypeInfo) {
			Class<?> typeClass = type.getTypeClass();
			if (typeClass == Double.class) {
				return 8;
			} else if (typeClass == Integer.class) {
				return 4;
			} else if (typeClass == Float.class) {
				return 4;
			} else if (typeClass == Long.class) {
				return 8;
			} else {
				throw new TornadoFlinkTypeRuntimeException("Flink-Tornado Data type not supported: " + typeClass);
			}
		} else if (type instanceof BasicArrayTypeInfo) {
			Class<?> typeClass = type.getTypeClass();
			if (typeClass.getName().contains("Float")) {
				return 4;
			} else if (typeClass.getName().contains("Double")) {
				return 8;
			} else if (typeClass.getName().contains("Integer")) {
				return 4;
			} else if (typeClass.getName().contains("Long")) {
				return 8;
		    } else {
				throw new TornadoFlinkTypeRuntimeException("Flink-Tornado Data type not supported: " + typeClass);
			}

		} else if (type.getClass() == PojoTypeInfo.class) {
			throw new TornadoFlinkTypeRuntimeException("ERROR: Provided type is not Numeric/Basic. Please call the corresponding function to the type provided.");
		} else {
			throw new TornadoFlinkTypeRuntimeException("Flink-Tornado Data type not supported yet: ");
		}
	}

	private static int arraySize (TypeInformation type, ArrayList arlist) {
		if (type instanceof BasicArrayTypeInfo) {
			Class<?> typeClass = type.getTypeClass();
			if (typeClass.getName().contains("Float")) {
				Float[] ar = (Float[]) arlist.get(0);
				return ar.length;
			} else if (typeClass.getName().contains("Double")) {
				Double[] ar = (Double[]) arlist.get(0);
				return ar.length;
			} else if (typeClass.getName().contains("Integer")) {
				Integer[] ar = (Integer[]) arlist.get(0);
				return ar.length;
			} else if (typeClass.getName().contains("Long")) {
				Long[] ar = (Long[]) arlist.get(0);
				return ar.length;
			} else {
				throw new TornadoFlinkTypeRuntimeException("Flink-Tornado Data type not supported: " + typeClass);
			}

		} else {
			throw new TornadoFlinkTypeRuntimeException("Flink-Tornado Data type not supported yet: ");
		}
	}
}
