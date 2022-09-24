package org.apache.flink.api.asm;

import org.apache.flink.api.java.tuple.Tuple2;
import org.objectweb.asm.*;
import org.objectweb.asm.util.TraceClassVisitor;

import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.Queue;
import java.util.StringTokenizer;

import static org.apache.flink.api.asm.ExamineUDF.setTypeVariablesMap;
import static org.apache.flink.api.asm.ExamineUDF.setTypeVariablesReduce;

public class AcceleratedFlinkUserFunction {
//	static private TornadoMap msk;
//	static private TornadoMap2 msk2;
//	static private TornadoMap3 msk3;
//	static private TornadoMap4 msk4;
//	static private TornadoReduce rsk;
	static private byte[] bytesmsk;
	static private byte[] bytesmsk2;
	static private byte[] bytesmsk3;
	static private byte[] bytesmsk4;
	static private byte[] bytesmsk5;
	static private byte[] bytesrsk;
	static private byte[] bytesrsk2;
	//static private Queue<TornadoMap> tmaps = new LinkedList<>();
	//static private Queue<TornadoReduce> treds = new LinkedList<>();

	public static void transformUDF(String name) {
		try {
			TransformUDF.mapUserClassName = name.replace("class ", "").replace(".", "/");
			if (TransformUDF.mapUserClassName.contains("Predict")) {
				TransformUDF.tornadoMapName = "org/apache/flink/api/asm/MapASMSkeleton3";
			} else {
				TransformUDF.tornadoMapName = "org/apache/flink/api/asm/MapASMSkeleton";
			}
			ExamineUDF.FlinkClassVisitor flinkVisit = new ExamineUDF.FlinkClassVisitor();
			ClassReader flinkClassReader = new ClassReader(TransformUDF.mapUserClassName);
			flinkClassReader.accept(flinkVisit, 0);

			setTypeVariablesMap();

			// ASM work for map
			// patch udf into the appropriate MapASMSkeleton
			String desc = "L" + TransformUDF.mapUserClassName + ";";
			ClassReader readerMap;
			if (TransformUDF.mapUserClassName.contains("Predict")) {
				readerMap = new ClassReader("org.apache.flink.api.asm.MapASMSkeleton4");
			} else {
				readerMap = new ClassReader("org.apache.flink.api.asm.MapASMSkeleton");
			}
			//ClassReader readerMap = new ClassReader("org.apache.flink.runtime.asm.map.MapASMSkeleton");
			ClassWriter writerMap = new ClassWriter(readerMap, ClassWriter.COMPUTE_MAXS);
			writerMap.visitField(Opcodes.ACC_PUBLIC, "udf", desc, null, null).visitEnd();
			//TraceClassVisitor printer = new TraceClassVisitor(writerMap, new PrintWriter(System.out));
			TransformUDF.MapClassAdapter adapterMap = new TransformUDF.MapClassAdapter(writerMap);
			readerMap.accept(adapterMap, ClassReader.EXPAND_FRAMES);
			// tornado
			byte[] b = writerMap.toByteArray();
			bytesmsk = b;
//			AsmClassLoader loader = new AsmClassLoader();
//			if (TransformUDF.mapUserClassName.contains("Predict")) {
//				Class<?> clazzMap = loader.defineClass("org.apache.flink.api.asm.MapASMSkeleton3", b);
//				MiddleMap3 md = (MiddleMap3) clazzMap.newInstance();
//				msk3 = new TornadoMap3(md);
//			} else {
//				Class<?> clazzMap = loader.defineClass("org.apache.flink.api.asm.MapASMSkeleton", b);
//				MiddleMap md = (MiddleMap) clazzMap.newInstance();
//				msk = new TornadoMap(md);
//			}
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public static void transformUDF2(String name) {
		try {
			TransformUDF.mapUserClassName = name.replace("class ", "").replace(".", "/");
			TransformUDF.tornadoMapName = "org/apache/flink/api/asm/MapASMSkeleton2";
			ExamineUDF.FlinkClassVisitor flinkVisit = new ExamineUDF.FlinkClassVisitor();
			ClassReader flinkClassReader = new ClassReader(TransformUDF.mapUserClassName);
			flinkClassReader.accept(flinkVisit, 0);

			setTypeVariablesMap();

			// ASM work for map
			// patch udf into the appropriate MapASMSkeleton
			String desc = "L" + TransformUDF.mapUserClassName + ";";
			ClassReader readerMap = new ClassReader("org.apache.flink.api.asm.MapASMSkeleton2");
			ClassWriter writerMap = new ClassWriter(readerMap, ClassWriter.COMPUTE_MAXS);
			writerMap.visitField(Opcodes.ACC_PUBLIC, "udf", desc, null, null).visitEnd();
			//TraceClassVisitor printer = new TraceClassVisitor(writerMap, new PrintWriter(System.out));
			TransformUDF.MapClassAdapter adapterMap = new TransformUDF.MapClassAdapter(writerMap);
			readerMap.accept(adapterMap, ClassReader.EXPAND_FRAMES);
			// tornado
			byte[] b = writerMap.toByteArray();
			bytesmsk2 = b;
//			AsmClassLoader loader = new AsmClassLoader();
//			Class<?> clazzMap = loader.defineClass("org.apache.flink.api.asm.MapASMSkeleton2", b);
//			MiddleMap2 md = (MiddleMap2) clazzMap.newInstance();
//			msk2 = new TornadoMap2(md);
		} catch (Exception e) {
			System.out.println(e);
		}
	}


	public static void transformUDF3(String name) {
		try {
			TransformUDF.mapUserClassName = name.replace("class ", "").replace(".", "/");
			TransformUDF.tornadoMapName = "org/apache/flink/api/asm/MapASMSkeleton3";
			ExamineUDF.FlinkClassVisitor flinkVisit = new ExamineUDF.FlinkClassVisitor();
			ClassReader flinkClassReader = new ClassReader(TransformUDF.mapUserClassName);
			flinkClassReader.accept(flinkVisit, 0);

			setTypeVariablesMap();

			// ASM work for map
			// patch udf into the appropriate MapASMSkeleton
			String desc = "L" + TransformUDF.mapUserClassName + ";";
			ClassReader readerMap = new ClassReader("org.apache.flink.api.asm.MapASMSkeleton3");
			ClassWriter writerMap = new ClassWriter(readerMap, ClassWriter.COMPUTE_MAXS);
			writerMap.visitField(Opcodes.ACC_PUBLIC, "udf", desc, null, null).visitEnd();
			//TraceClassVisitor printer = new TraceClassVisitor(writerMap, new PrintWriter(System.out));
			TransformUDF.MapClassAdapter adapterMap = new TransformUDF.MapClassAdapter(writerMap);
			readerMap.accept(adapterMap, ClassReader.EXPAND_FRAMES);
			// tornado
			byte[] b = writerMap.toByteArray();
			bytesmsk3 = b;
//			AsmClassLoader loader = new AsmClassLoader();
//			Class<?> clazzMap = loader.defineClass("org.apache.flink.api.asm.MapASMSkeleton3", b);
//			MiddleMap3 md = (MiddleMap3) clazzMap.newInstance();
//			msk3 = new TornadoMap3(md);
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public static void transformUDF4(String name) {
		try {
			TransformUDF.mapUserClassName = name.replace("class ", "").replace(".", "/");
			TransformUDF.tornadoMapName = "org/apache/flink/api/asm/MapASMSkeleton4";
			ExamineUDF.FlinkClassVisitor flinkVisit = new ExamineUDF.FlinkClassVisitor();
			ClassReader flinkClassReader = new ClassReader(TransformUDF.mapUserClassName);
			flinkClassReader.accept(flinkVisit, 0);

			setTypeVariablesMap();

			// ASM work for map
			// patch udf into the appropriate MapASMSkeleton
			String desc = "L" + TransformUDF.mapUserClassName + ";";
			ClassReader readerMap = new ClassReader("org.apache.flink.api.asm.MapASMSkeleton4");
			ClassWriter writerMap = new ClassWriter(readerMap, ClassWriter.COMPUTE_MAXS);
			writerMap.visitField(Opcodes.ACC_PUBLIC, "udf", desc, null, null).visitEnd();
			//TraceClassVisitor printer = new TraceClassVisitor(writerMap, new PrintWriter(System.out));
			TransformUDF.MapClassAdapter adapterMap = new TransformUDF.MapClassAdapter(writerMap);
			readerMap.accept(adapterMap, ClassReader.EXPAND_FRAMES);
			// tornado
			byte[] b = writerMap.toByteArray();
			bytesmsk4 = b;
//			AsmClassLoader loader = new AsmClassLoader();
//			Class<?> clazzMap = loader.defineClass("org.apache.flink.api.asm.MapASMSkeleton4", b);
//			MiddleMap4 md = (MiddleMap4) clazzMap.newInstance();
//			msk4 = new TornadoMap4(md);
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public static void transformUDF5(String name) {
		try {
			TransformUDF.mapUserClassName = name.replace("class ", "").replace(".", "/");
			TransformUDF.tornadoMapName = "org/apache/flink/api/asm/MapASMSkeleton5";
			ExamineUDF.FlinkClassVisitor flinkVisit = new ExamineUDF.FlinkClassVisitor();
			ClassReader flinkClassReader = new ClassReader(TransformUDF.mapUserClassName);
			flinkClassReader.accept(flinkVisit, 0);

			setTypeVariablesMap();

			// ASM work for map
			// patch udf into the appropriate MapASMSkeleton
			String desc = "L" + TransformUDF.mapUserClassName + ";";
			ClassReader readerMap = new ClassReader("org.apache.flink.api.asm.MapASMSkeleton5");
			ClassWriter writerMap = new ClassWriter(readerMap, ClassWriter.COMPUTE_MAXS);
			writerMap.visitField(Opcodes.ACC_PUBLIC, "udf", desc, null, null).visitEnd();
			//TraceClassVisitor printer = new TraceClassVisitor(writerMap, new PrintWriter(System.out));
			TransformUDF.MapClassAdapter adapterMap = new TransformUDF.MapClassAdapter(writerMap);
			readerMap.accept(adapterMap, ClassReader.EXPAND_FRAMES);
			// tornado
			byte[] b = writerMap.toByteArray();
			bytesmsk5 = b;
//			AsmClassLoader loader = new AsmClassLoader();
//			Class<?> clazzMap = loader.defineClass("org.apache.flink.api.asm.MapASMSkeleton4", b);
//			MiddleMap4 md = (MiddleMap4) clazzMap.newInstance();
//			msk4 = new TornadoMap4(md);
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public static void transformReduceUDF(String name) {
		try {
			TransformUDF.redUserClassName = name.replace("class ", "").replace(".", "/");
			TransformUDF.tornadoReduceName = "org/apache/flink/api/asm/ReduceASMSkeleton";
			// examine udf
			ExamineUDF.FlinkClassVisitor flinkVisit = new ExamineUDF.FlinkClassVisitor();
			ClassReader flinkClassReader = new ClassReader(TransformUDF.redUserClassName);
			flinkClassReader.accept(flinkVisit, 0);

			setTypeVariablesReduce();

			// ASM work for reduce
			ClassReader readerRed = new ClassReader("org.apache.flink.api.asm.ReduceASMSkeleton");
			ClassWriter writerRed = new ClassWriter(readerRed, ClassWriter.COMPUTE_MAXS);
			//TraceClassVisitor printerRed = new TraceClassVisitor(writerRed, new PrintWriter(System.out));
			// to remove debugging info, just replace the printer in class adapter call with
			// the writer
			TransformUDF.ReduceClassAdapter adapterRed = new TransformUDF.ReduceClassAdapter(writerRed);
			readerRed.accept(adapterRed, ClassReader.EXPAND_FRAMES);
			byte[] b = writerRed.toByteArray();
			bytesrsk = b;
//			AsmClassLoader loader = new AsmClassLoader();
//			Class<?> clazzRed = loader.defineClass("org.apache.flink.api.asm.ReduceASMSkeleton", b);
//			MiddleReduce mdr = (MiddleReduce) clazzRed.newInstance();
//			rsk = new TornadoReduce(mdr);
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public static void transformReduceUDF2(String name) {
		try {
			TransformUDF.redUserClassName = name.replace("class ", "").replace(".", "/");
			TransformUDF.tornadoReduceName = "org/apache/flink/api/asm/ReduceASMSkeleton2";
			// examine udf
			ExamineUDF.FlinkClassVisitor flinkVisit = new ExamineUDF.FlinkClassVisitor();
			ClassReader flinkClassReader = new ClassReader(TransformUDF.redUserClassName);
			flinkClassReader.accept(flinkVisit, 0);

			setTypeVariablesReduce();

			// ASM work for reduce
			ClassReader readerRed = new ClassReader("org.apache.flink.api.asm.ReduceASMSkeleton2");
			ClassWriter writerRed = new ClassWriter(readerRed, ClassWriter.COMPUTE_MAXS);
			//TraceClassVisitor printerRed = new TraceClassVisitor(writerRed, new PrintWriter(System.out));
			// to remove debugging info, just replace the printer in class adapter call with
			// the writer
			TransformUDF.ReduceClassAdapter adapterRed = new TransformUDF.ReduceClassAdapter(writerRed);
			readerRed.accept(adapterRed, ClassReader.EXPAND_FRAMES);
			byte[] b = writerRed.toByteArray();
			bytesrsk2 = b;
//			AsmClassLoader loader = new AsmClassLoader();
//			Class<?> clazzRed = loader.defineClass("org.apache.flink.api.asm.ReduceASMSkeleton", b);
//			MiddleReduce mdr = (MiddleReduce) clazzRed.newInstance();
//			rsk = new TornadoReduce(mdr);
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public static class GetTypesMap extends ClassVisitor {

		private static String descFunc;

		public GetTypesMap() {
			super(Opcodes.ASM6);
		}


		@Override
		public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
			if (access == 1 && !(name.contains("<init>"))) {
				descFunc = desc;
			}
			return super.visitMethod(access, name, desc, signature, exceptions);
		}

		public static Tuple2<String, String> getTypes (String name) {
			try {
				String mapUserClassName = name.replace("class ", "").replace(".", "/");
				GetTypesMap visitor = new GetTypesMap();
				ClassReader reader = new ClassReader(mapUserClassName);
				reader.accept(visitor, 0);
				Tuple2<String, String> types = mapTypes(descFunc);
				return types;
			} catch (Exception e) {
				System.out.println(e);
				return null;
			}
		}
	}


	public static class GetTypesReduce extends ClassVisitor {

		private static String descFunc;

		public GetTypesReduce() {
			super(Opcodes.ASM6);
		}


		@Override
		public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
			if (access == 1 && !(name.contains("<init>"))) {
				descFunc = desc;
			}
			return super.visitMethod(access, name, desc, signature, exceptions);
		}

		public static Tuple2<String, String> getTypes (String name) {
			try {
				String redUserClassName = name.replace("class ", "").replace(".", "/");
				GetTypesReduce visitor = new GetTypesReduce();
				ClassReader reader = new ClassReader(redUserClassName);
				reader.accept(visitor, 0);
				Tuple2<String, String> types = reduceTypes(descFunc);
				return types;
			} catch (Exception e) {
				System.out.println(e);
				return null;
			}
		}
	}


	public static Tuple2<String, String> mapTypes(String descFunc) throws Exception {
		String delims = "()";
		StringTokenizer tok = new StringTokenizer(descFunc, delims);
		String argType;
		String returnType;
		String[] types = new String[2];
		int i = 0;
		while (tok.hasMoreElements()) {
			types[i] = (String) tok.nextElement();
			i++;
		}
		argType = types[0];
		returnType = types[1];
		Tuple2<String, String> inOutTypes = new Tuple2<>();
		// set everything related to the argument type
		if (argType.contains("[Ljava/lang/Float")) {
			inOutTypes.f0 = "java/lang/Float[]";
		} else if (argType.contains("Integer")) {
			inOutTypes.f0 = "java/lang/Integer";
		} else if (argType.contains("Double")) {
			inOutTypes.f0 = "java/lang/Double";
		} else if (argType.contains("Long")) {
			inOutTypes.f0 = "java/lang/Long";
		} else if (argType.contains("Float")) {
			inOutTypes.f0 = "java/lang/Float";
		} else if (argType.contains("Tuple2")) {
			inOutTypes.f0 = "org/apache/flink/api/java/tuple/Tuple2";
		} else if (argType.contains("Tuple3")) {
			inOutTypes.f0 = "org/apache/flink/api/java/tuple/Tuple3";
		} else if (argType.contains("Tuple4")) {
			inOutTypes.f0 = "org/apache/flink/api/java/tuple/Tuple4";
		} else {
			throw new Exception("Argument type " + argType + " not supported on TornadoVM yet.");
		}
		// set everything related to the return type
		if (returnType.contains("[Ljava/lang/Float")) {
			inOutTypes.f1 = "java/lang/Float[]";
		} else if (returnType.contains("Integer")) {
			inOutTypes.f1 = "java/lang/Integer";
		} else if (returnType.contains("Double")) {
			inOutTypes.f1 = "java/lang/Double";
		} else if (returnType.contains("Long")) {
			inOutTypes.f1 ="java/lang/Long";
		} else if (returnType.contains("Float")) {
			inOutTypes.f1 = "java/lang/Float";
		} else if (returnType.contains("Tuple2")) {
			inOutTypes.f1 = "org/apache/flink/api/java/tuple/Tuple2";
		} else if (returnType.contains("Tuple3")) {
			inOutTypes.f1 = "org/apache/flink/api/java/tuple/Tuple3";
		} else if (returnType.contains("Tuple4")) {
			inOutTypes.f1 = "org/apache/flink/api/java/tuple/Tuple4";
		}
		return inOutTypes;
	}


	// based on the information extracted from the visitor, the necessary variables are initialized
	public static Tuple2<String, String> reduceTypes(String descFunc) throws Exception {
		String delims = "()";
		StringTokenizer tok = new StringTokenizer(descFunc, delims);
		String argType;
		String returnType;
		String[] types = new String[2];
		int i = 0;
		while (tok.hasMoreElements()) {
			types[i] = (String) tok.nextElement();
			i++;
		}
		argType = types[0];
		returnType = types[1];
		// set everything related to the argument type
		Tuple2<String, String> inOutTypes = new Tuple2<>();
		if (argType.contains("Integer")) {
			inOutTypes.f0 = "java/lang/Integer";
		} else if (argType.contains("Double")) {
			inOutTypes.f0 = "java/lang/Double";
		} else if (argType.contains("Long")) {
			inOutTypes.f0 = "java/lang/Long";
		} else if (argType.contains("Float")) {
			inOutTypes.f0 = "java/lang/Float";
		} else if (argType.contains("Tuple2")) {
			inOutTypes.f0 = "org/apache/flink/api/java/tuple/Tuple2";
		}	else if (argType.contains("Tuple3")) {
			inOutTypes.f0 = "org/apache/flink/api/java/tuple/Tuple3";
			//throw new Exception("Argument type " + argType + " not supported on TornadoVM yet.");
		} else if (argType.contains("Tuple4")) {
			inOutTypes.f0 = "org/apache/flink/api/java/tuple/Tuple4";
		} else {
			throw new Exception("Argument type " + argType + " not supported on TornadoVM yet.");
		}
		// set everything related to the return type
		if (returnType.contains("Integer")) {
			inOutTypes.f1 = "java/lang/Integer";
		} else if (returnType.contains("Double")) {
			inOutTypes.f1 = "java/lang/Double";
		} else if (returnType.contains("Long")) {
			inOutTypes.f1 = "java/lang/Long";
		} else if (returnType.contains("Float")) {
			inOutTypes.f1 = "java/lang/Float";
		} else if (returnType.contains("Tuple2")){
			inOutTypes.f1 = "org/apache/flink/api/java/tuple/Tuple2";
		} else if (returnType.contains("Tuple3")) {
			inOutTypes.f1 = "org/apache/flink/api/java/tuple/Tuple3";
		} else if (returnType.contains("Tuple4")) {
			inOutTypes.f1 = "org/apache/flink/api/java/tuple/Tuple4";
		}
		return inOutTypes;
	}

	public static byte[] getTornadoMapBytes() {
		return bytesmsk;
	}

	public static byte[] getTornadoMap2Bytes() {
		return bytesmsk2;
	}

	public static byte[] getTornadoMap3Bytes() {
		return bytesmsk3;
	}

	public static byte[] getTornadoMap4Bytes() {
		return bytesmsk4;
	}

	public static byte[] getTornadoMap5Bytes() {
		return bytesmsk5;
	}

	public static byte[] getTornadoReduceBytes() {
		return bytesrsk;
	}

	public static byte[] getTornadoReduceBytes2() {
		return bytesrsk2;
	}

}
