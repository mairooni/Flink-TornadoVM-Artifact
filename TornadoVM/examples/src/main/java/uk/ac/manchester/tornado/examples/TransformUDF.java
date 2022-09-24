package uk.ac.manchester.tornado.examples;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.LocalVariablesSorter;

import static uk.ac.manchester.tornado.examples.ExamineUDF.*;

public class TransformUDF {
    public static String mapUserClassName;
    public static String redUserClassName;
    public static String tornadoMapName;

    /**
     * This class initiates the transformation by spotting the appropriate function
     * in the skeleton and calling the adapter for it.
     */
    public static class MapClassAdapter extends ClassVisitor {
        public MapClassAdapter(ClassVisitor cv) {
            super(Opcodes.ASM6, cv);
        }

        @Override
        public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
            MethodVisitor mv = cv.visitMethod(access, name, desc, signature, exceptions);
            // find the appropriate skeleton for the udf
            if (name.contains("map") && desc.contains(skeletonMapDesc)) {
                mv = new MapMethodAdapter(access, desc, mv);
                return new MapRemoveReturn(access, desc, mv);
            } else if (name.contains("init")) {
                return new MapConstructorAdapter(access, desc, mv);
            } else {
                return mv;
            }
        }
    }

    /**
     * Removes the return 0 from the MapASMSkeleton.
     */
    public static class MapRemoveReturn extends LocalVariablesSorter {

        public MapRemoveReturn(int access, String desc, MethodVisitor mv) {
            super(Opcodes.ASM6, access, desc, mv);
        }

        @Override
        public void visitInsn(int opcode) {
            if (opcode == gConst) {
                super.visitInsn(gReturn);
            }

        }

    }

    /**
     * Inserts the udf call in the appropriate map function of the skeleton.
     */
    public static class MapMethodAdapter extends LocalVariablesSorter {

        public MapMethodAdapter(int access, String desc, MethodVisitor mv) {
            super(Opcodes.ASM6, access, desc, mv);
        }

        @Override
        public void visitCode() {
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            // mv.visitFieldInsn(Opcodes.GETFIELD,
            // "org/apache/flink/runtime/asm/map/MapASMSkeleton", "udf", "L" +
            // mapUserClassName + ";");
            mv.visitFieldInsn(Opcodes.GETFIELD, tornadoMapName, "udf", "L" + mapUserClassName + ";");
            mv.visitVarInsn(gLoad, 1);
            if (!inOwner.contains("Tuple")) {
                mv.visitMethodInsn(Opcodes.INVOKESTATIC, inOwner, "valueOf", inDesc, false);
            }
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, mapUserClassName, "map", descFunc, false);
            if (!outOwner.contains("Tuple")) {
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, outOwner, outName, outDesc, false);
            }
            super.visitCode();
        }

        @Override
        public void visitMaxs(int maxStack, int maxLocals) {

            super.visitMaxs(maxStack, maxLocals + 1);
        }

    }

    /**
     * This class creates a field in the skeleton class, which will be used to call
     * the user function on the map skeleton.
     */
    public static class MapConstructorAdapter extends LocalVariablesSorter {

        public MapConstructorAdapter(int access, String desc, MethodVisitor mv) {
            super(Opcodes.ASM6, access, desc, mv);
        }

        @Override
        public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf) {
            super.visitMethodInsn(opcode, owner, name, desc, itf);
            if (Opcodes.INVOKESPECIAL == opcode) {
                mv.visitVarInsn(Opcodes.ALOAD, 0);
                mv.visitTypeInsn(Opcodes.NEW, mapUserClassName);
                mv.visitInsn(Opcodes.DUP);
                mv.visitMethodInsn(Opcodes.INVOKESPECIAL, mapUserClassName, "<init>", "()V", false);
                // mv.visitFieldInsn(Opcodes.PUTFIELD,
                // "org/apache/flink/runtime/asm/map/MapASMSkeleton", "udf", "L" +
                // mapUserClassName + ";");
                mv.visitFieldInsn(Opcodes.PUTFIELD, tornadoMapName, "udf", "L" + mapUserClassName + ";");
            }

        }

        @Override
        public void visitMaxs(int maxStack, int maxLocals) {
            super.visitMaxs(maxStack, maxLocals + 1);
        }

    }

    /**
     * This class initiates the transformation by spotting the appropriate function
     * in the skeleton and calling the adapter for it.
     */
    public static class ReduceClassAdapter extends ClassVisitor {
        public ReduceClassAdapter(ClassVisitor cv) {
            super(Opcodes.ASM6, cv);
        }

        @Override
        public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
            MethodVisitor mv = cv.visitMethod(access, name, desc, signature, exceptions);

            // find the appropriate skeleton for the udf
            if (name.contains("red") && desc.contains(skeletonRedDesc)) {
                mv = new ReduceMethodAdapter(access, desc, mv);
                return new ReduceRemoveReturn(access, desc, mv);
            } else if (name.contains("init")) {
                cv.visitField(Opcodes.ACC_PUBLIC, "udf", "L" + redUserClassName + ";", null, null).visitEnd();
                return new ReduceConstructorAdapter(access, desc, mv);
            } else {
                return mv;
            }

        }
    }

    /**
     * Removes the return 0 from the ReduceASMSkeleton.
     */
    public static class ReduceRemoveReturn extends LocalVariablesSorter {

        public ReduceRemoveReturn(int access, String desc, MethodVisitor mv) {
            super(Opcodes.ASM6, access, desc, mv);
        }

        @Override
        public void visitInsn(int opcode) {
            if (opcode == gConst) {
                super.visitInsn(gReturn);
            }

        }

    }

    /**
     * This class creates a field in the skeleton class, which will be used to call
     * the user function on the reduce skeleton.
     */
    public static class ReduceConstructorAdapter extends LocalVariablesSorter {

        public ReduceConstructorAdapter(int access, String desc, MethodVisitor mv) {
            super(Opcodes.ASM6, access, desc, mv);
        }

        @Override
        public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf) {
            super.visitMethodInsn(opcode, owner, name, desc, itf);
            if (Opcodes.INVOKESPECIAL == opcode) {
                mv.visitVarInsn(Opcodes.ALOAD, 0);
                mv.visitTypeInsn(Opcodes.NEW, redUserClassName);
                mv.visitInsn(Opcodes.DUP);
                mv.visitMethodInsn(Opcodes.INVOKESPECIAL, redUserClassName, "<init>", "()V", false);
                mv.visitFieldInsn(Opcodes.PUTFIELD, "org/apache/flink/runtime/asm/reduce/ReduceASMSkeleton", "udf", "L" + redUserClassName + ";");
            }

        }

        @Override
        public void visitMaxs(int maxStack, int maxLocals) {
            super.visitMaxs(maxStack, maxLocals + 1);
        }

    }

    /**
     * Inserts the udf call in the appropriate map function of the skeleton.
     */
    public static class ReduceMethodAdapter extends LocalVariablesSorter {

        public ReduceMethodAdapter(int access, String desc, MethodVisitor mv) {
            super(Opcodes.ASM6, access, desc, mv);
        }

        @Override
        public void visitCode() {
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitFieldInsn(Opcodes.GETFIELD, "org/apache/flink/runtime/asm/reduce/ReduceASMSkeleton", "udf", "L" + redUserClassName + ";");
            mv.visitVarInsn(gLoad, 1);
            if (!inOwner.contains("Tuple")) {
                mv.visitMethodInsn(Opcodes.INVOKESTATIC, inOwner, "valueOf", inDesc, false);
            }
            mv.visitVarInsn(gLoad, loadSecondFieldRed);
            if (!inOwner.contains("Tuple")) {
                mv.visitMethodInsn(Opcodes.INVOKESTATIC, inOwner, "valueOf", inDesc, false);
            }
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, redUserClassName, "reduce", descFunc, false);
            if (!outOwner.contains("Tuple")) {
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, outOwner, outName, outDesc, false);
            }
            super.visitCode();
        }

        @Override
        public void visitMaxs(int maxStack, int maxLocals) {

            super.visitMaxs(maxStack, maxLocals + 1);
        }

    }

}
