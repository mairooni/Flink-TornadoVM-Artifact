package org.apache.flink.api.asm;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.util.StringTokenizer;

/**
 * This class examines the user function and extracts the necessary information
 * for the bytecode manipulation.
 */
public class ExamineUDF {

	// Type of function that user class contains, i.e. map, reduce etc...
	public static String functionType;
	// description of udf
	public static String descFunc;
	// (I | D | L | F )LOAD
	public static int gLoad;
	// (I | D | L | F )RETURN
	public static int gReturn;
	// (I | D | L | F)CONST
	public static int gConst;
	// descriptor that helps us call the correct function from the skeleton
	public static String skeletonMapDesc;

	public static String skeletonRedDesc;
	// for valueOf
	public static String inOwner;
	public static String inDesc;
	// for (long | double | int | float)value
	public static String outOwner;
	public static String outName;
	public static String outDesc;
	// variable to help us call the appropriate tornado map method
	public static String tornadoMapMethod;
	// variable to help us call the appropriate tornado reduce method
	public static String tornadoRedMethod;

	public static int loadSecondFieldRed;

	/**
	 * Visitor for the appropriate map function of the MapASMSkeleton.
	 */
	public static class FlinkClassVisitor extends ClassVisitor {
		public FlinkClassVisitor() {
			super(Opcodes.ASM6);
		}

		@Override
		public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
			if (access == 1 && !(name.contains("<init>"))) {
				functionType = name;
				descFunc = desc;
			}
			return super.visitMethod(access, name, desc, signature, exceptions);
		}
	}

	// based on the information extracted from the visitor, the necessary variables are initialized
	public static void setTypeVariablesMap() throws Exception {
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
		if (argType.contains("[Ljava/lang/Float")) {
			gLoad = Opcodes.ALOAD;
			inOwner = "java/lang/Float[]";
			//inDesc = "(F)Ljava/lang/Float;";
			skeletonMapDesc = "([Ljava/lang/Float;)";
			tornadoMapMethod = "Float[][],";
		} else if (argType.contains("Integer")) {
			gLoad = Opcodes.ILOAD;
			inOwner = "java/lang/Integer";
			inDesc = "(I)Ljava/lang/Integer;";
			skeletonMapDesc = "(I)";
			tornadoMapMethod = "int[],";
		} else if (argType.contains("Double")) {
			gLoad = Opcodes.DLOAD;
			inOwner = "java/lang/Double";
			inDesc = "(D)Ljava/lang/Double;";
			skeletonMapDesc = "(D)";
			tornadoMapMethod = "double[],";
		} else if (argType.contains("Long")) {
			gLoad = Opcodes.LLOAD;
			inOwner = "java/lang/Long";
			inDesc = "(J)Ljava/lang/Long;";
			skeletonMapDesc = "(J)";
			tornadoMapMethod = "long[],";
		} else if (argType.contains("Float")) {
			gLoad = Opcodes.FLOAD;
			inOwner = "java/lang/Float";
			inDesc = "(F)Ljava/lang/Float;";
			skeletonMapDesc = "(F)";
			tornadoMapMethod = "float[],";
		} else if (argType.contains("Tuple2")) {
			gLoad = Opcodes.ALOAD;
			inOwner = "org/apache/flink/api/java/tuple/Tuple2";
			//inDesc = "(Lorg/apache/flink/api/java/tuple/Tuple2)Lorg/apache/flink/api/java/tuple/Tuple2;";
			skeletonMapDesc = "(Lorg/apache/flink/api/java/tuple/Tuple2;)";
			tornadoMapMethod = "Tuple2[],";
			//throw new Exception("Argument type " + argType + " not supported on TornadoVM yet.");
		} else if (argType.contains("Tuple3")) {
			gLoad = Opcodes.ALOAD;
			inOwner = "org/apache/flink/api/java/tuple/Tuple3";
			//inDesc = "(Lorg/apache/flink/api/java/tuple/Tuple2)Lorg/apache/flink/api/java/tuple/Tuple2;";
			skeletonMapDesc = "(Lorg/apache/flink/api/java/tuple/Tuple3;)";
			tornadoMapMethod = "Tuple3[],";
		} else if (argType.contains("Tuple4")) {
			gLoad = Opcodes.ALOAD;
			inOwner = "org/apache/flink/api/java/tuple/Tuple4";
			//inDesc = "(Lorg/apache/flink/api/java/tuple/Tuple2)Lorg/apache/flink/api/java/tuple/Tuple2;";
			skeletonMapDesc = "(Lorg/apache/flink/api/java/tuple/Tuple4;)";
			tornadoMapMethod = "Tuple4[],";
		} else {
			throw new Exception("Argument type " + argType + " not supported on TornadoVM yet.");
		}
		// set everything related to the return type
		if (returnType.contains("[Ljava/lang/Float")) {
			gReturn = Opcodes.ARETURN;
			gConst = Opcodes.ACONST_NULL;
			outOwner = "java/lang/Float[]";
			//outName = "intValue";
			//outDesc = "()I";
			skeletonMapDesc = skeletonMapDesc + "[Ljava/lang/Float;";
			tornadoMapMethod = tornadoMapMethod + "Float[][]";
		} else if (returnType.contains("Integer")) {
			gReturn = Opcodes.IRETURN;
			gConst = Opcodes.ICONST_0;
			outOwner = "java/lang/Integer";
			outName = "intValue";
			outDesc = "()I";
			skeletonMapDesc = skeletonMapDesc + "I";
			tornadoMapMethod = tornadoMapMethod + "int[]";
		} else if (returnType.contains("Double")) {
			gReturn = Opcodes.DRETURN;
			gConst = Opcodes.DCONST_0;
			outOwner = "java/lang/Double";
			outName = "doubleValue";
			outDesc = "()D";
			skeletonMapDesc = skeletonMapDesc + "D";
			tornadoMapMethod = tornadoMapMethod + "double[]";
		} else if (returnType.contains("Long")) {
			gReturn = Opcodes.LRETURN;
			gConst = Opcodes.LCONST_0;
			outOwner = "java/lang/Long";
			outName = "longValue";
			outDesc = "()J";
			skeletonMapDesc = skeletonMapDesc + "J";
			tornadoMapMethod = tornadoMapMethod + "long[]";
		} else if (returnType.contains("Float")) {
			gReturn = Opcodes.FRETURN;
			gConst = Opcodes.FCONST_0;
			outOwner = "java/lang/Float";
			outName = "floatValue";
			outDesc = "()F";
			skeletonMapDesc = skeletonMapDesc + "F";
			tornadoMapMethod = tornadoMapMethod + "float[]";
		} else if (returnType.contains("Tuple2")) {
			gReturn = Opcodes.ARETURN;
			gConst = Opcodes.ACONST_NULL;
			outOwner = "org/apache/flink/api/java/tuple/Tuple2";
			//inDesc = "(Lorg/apache/flink/api/java/tuple/Tuple2)Lorg/apache/flink/api/java/tuple/Tuple2;";
			skeletonMapDesc = skeletonMapDesc + "Lorg/apache/flink/api/java/tuple/Tuple2;";
			tornadoMapMethod = tornadoMapMethod + "Tuple2[]";
			//throw new Exception("Return type " + returnType + " not supported on TornadoVM yet.");
		} else if (returnType.contains("Tuple3")) {
			gReturn = Opcodes.ARETURN;
			gConst = Opcodes.ACONST_NULL;
			outOwner = "org/apache/flink/api/java/tuple/Tuple3";
			skeletonMapDesc = skeletonMapDesc + "Lorg/apache/flink/api/java/tuple/Tuple3;";
			tornadoMapMethod = tornadoMapMethod + "Tuple3[]";
			//throw new Exception("Return type " + returnType + " not supported on TornadoVM yet.");
		} else if (returnType.contains("Tuple4")) {
			gReturn = Opcodes.ARETURN;
			gConst = Opcodes.ACONST_NULL;
			outOwner = "org/apache/flink/api/java/tuple/Tuple4";
			skeletonMapDesc = skeletonMapDesc + "Lorg/apache/flink/api/java/tuple/Tuple4;";
			tornadoMapMethod = tornadoMapMethod + "Tuple4[]";
			//throw new Exception("Return type " + returnType + " not supported on TornadoVM yet.");
		}
	}


	// based on the information extracted from the visitor, the necessary variables are initialized
	public static void setTypeVariablesReduce() throws Exception {
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
		if (argType.contains("Integer")) {
			gLoad = Opcodes.ILOAD;
			inOwner = "java/lang/Integer";
			inDesc = "(I)Ljava/lang/Integer;";
			skeletonRedDesc = "(II)";
			tornadoRedMethod = "int[],";
			loadSecondFieldRed = 2;
		} else if (argType.contains("Double")) {
			gLoad = Opcodes.DLOAD;
			inOwner = "java/lang/Double";
			inDesc = "(D)Ljava/lang/Double;";
			skeletonRedDesc = "(DD)";
			tornadoRedMethod = "double[],";
			loadSecondFieldRed = 3;
		} else if (argType.contains("Long")) {
			gLoad = Opcodes.LLOAD;
			inOwner = "java/lang/Long";
			inDesc = "(J)Ljava/lang/Long;";
			skeletonRedDesc = "(JJ)";
			tornadoRedMethod = "long[],";
			loadSecondFieldRed = 3;
		} else if (argType.contains("Float")) {
			gLoad = Opcodes.FLOAD;
			inOwner = "java/lang/Float";
			inDesc = "(F)Ljava/lang/Float;";
			skeletonRedDesc = "(FF)";
			tornadoRedMethod = "float[],";
			loadSecondFieldRed = 2;
		} else if (argType.contains("Tuple2")) {
			gLoad = Opcodes.ALOAD;
			inOwner = "org/apache/flink/api/java/tuple/Tuple2";
			//inDesc = "(Lorg/apache/flink/api/java/tuple/Tuple2)Lorg/apache/flink/api/java/tuple/Tuple2;";
			skeletonRedDesc = "(Lorg/apache/flink/api/java/tuple/Tuple2;Lorg/apache/flink/api/java/tuple/Tuple2;)";
			tornadoRedMethod = "Tuple2[],";
			loadSecondFieldRed = 2;
			//throw new Exception("Argument type " + argType + " not supported on TornadoVM yet.");
		}	else if (argType.contains("Tuple3")) {
			gLoad = Opcodes.ALOAD;
			inOwner = "org/apache/flink/api/java/tuple/Tuple3";
			//inDesc = "(Lorg/apache/flink/api/java/tuple/Tuple2)Lorg/apache/flink/api/java/tuple/Tuple2;";
			skeletonRedDesc = "(Lorg/apache/flink/api/java/tuple/Tuple3;Lorg/apache/flink/api/java/tuple/Tuple3;)";
			tornadoRedMethod = "Tuple3[],";
			loadSecondFieldRed = 2;
			//throw new Exception("Argument type " + argType + " not supported on TornadoVM yet.");
		} else if (argType.contains("Tuple4")) {
			gLoad = Opcodes.ALOAD;
			inOwner = "org/apache/flink/api/java/tuple/Tuple4";
			//inDesc = "(Lorg/apache/flink/api/java/tuple/Tuple2)Lorg/apache/flink/api/java/tuple/Tuple2;";
			skeletonRedDesc = "(Lorg/apache/flink/api/java/tuple/Tuple4;Lorg/apache/flink/api/java/tuple/Tuple4;)";
			tornadoRedMethod = "Tuple4[],";
			loadSecondFieldRed = 2;
			//throw new Exception("Argument type " + argType + " not supported on TornadoVM yet.");
		} else {
			throw new Exception("Argument type " + argType + " not supported on TornadoVM yet.");
		}
		// set everything related to the return type
		if (returnType.contains("Integer")) {
			gReturn = Opcodes.IRETURN;
			gConst = Opcodes.ICONST_0;
			outOwner = "java/lang/Integer";
			outName = "intValue";
			outDesc = "()I";
			skeletonRedDesc = skeletonRedDesc + "I";
			tornadoRedMethod = tornadoRedMethod + "int[]";
		} else if (returnType.contains("Double")) {
			gReturn = Opcodes.DRETURN;
			gConst = Opcodes.DCONST_0;
			outOwner = "java/lang/Double";
			outName = "doubleValue";
			outDesc = "()D";
			skeletonRedDesc = skeletonRedDesc + "D";
			tornadoRedMethod = tornadoRedMethod + "double[]";
		} else if (returnType.contains("Long")) {
			gReturn = Opcodes.LRETURN;
			gConst = Opcodes.LCONST_0;
			outOwner = "java/lang/Long";
			outName = "longValue";
			outDesc = "()J";
			skeletonRedDesc = skeletonRedDesc + "J";
			tornadoRedMethod = tornadoRedMethod + "long[]";
		} else if (returnType.contains("Float")) {
			gReturn = Opcodes.FRETURN;
			gConst = Opcodes.FCONST_0;
			outOwner = "java/lang/Float";
			outName = "floatValue";
			outDesc = "()F";
			skeletonRedDesc = skeletonRedDesc + "F";
			tornadoRedMethod = tornadoRedMethod + "float[]";
		} else if (returnType.contains("Tuple2")){
			gReturn = Opcodes.ARETURN;
			gConst = Opcodes.ACONST_NULL;
			outOwner = "org/apache/flink/api/java/tuple/Tuple2";
			//inDesc = "(Lorg/apache/flink/api/java/tuple/Tuple2)Lorg/apache/flink/api/java/tuple/Tuple2;";
			skeletonRedDesc = skeletonRedDesc + "Lorg/apache/flink/api/java/tuple/Tuple2;";
			tornadoRedMethod = tornadoRedMethod + "Tuple2[]";
			//throw new Exception("Return type " + returnType + " not supported on TornadoVM yet.");
		} else if (returnType.contains("Tuple3")) {
			gReturn = Opcodes.ARETURN;
			gConst = Opcodes.ACONST_NULL;
			outOwner = "org/apache/flink/api/java/tuple/Tuple3";
			//inDesc = "(Lorg/apache/flink/api/java/tuple/Tuple2)Lorg/apache/flink/api/java/tuple/Tuple2;";
			skeletonRedDesc = skeletonRedDesc + "Lorg/apache/flink/api/java/tuple/Tuple3;";
			tornadoRedMethod = tornadoRedMethod + "Tuple3[]";
		} else if (returnType.contains("Tuple4")) {
			gReturn = Opcodes.ARETURN;
			gConst = Opcodes.ACONST_NULL;
			outOwner = "org/apache/flink/api/java/tuple/Tuple4";
			//inDesc = "(Lorg/apache/flink/api/java/tuple/Tuple2)Lorg/apache/flink/api/java/tuple/Tuple2;";
			skeletonRedDesc = skeletonRedDesc + "Lorg/apache/flink/api/java/tuple/Tuple4;";
			tornadoRedMethod = tornadoRedMethod + "Tuple4[]";
		}
	}

}

