package uk.ac.manchester.tornado.examples;

import org.objectweb.asm.*;
import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.annotations.Parallel;

import java.util.Arrays;

/**
 * This example class simulates the sketcher exception that occurs on the
 * Tornado-Flink integration when we run more than one task schedule with the
 * same ASM function signature.
 * 
 * This class demonstrates three cases: 1) Run with two threads: exception is
 * thrown 2) Run with two threads but execute a simple function before: no
 * exception 3) Run with one thread: no exception
 */
public class TestSketcher {

    public static String userClassName = "uk/ac/manchester/tornado/examples/FlinkMapUDF$Flink";

    public static class Test {

        public void test(int[] in, int[] out) {
            for (@Parallel int i = 0; i < in.length; i++) {
                out[i] = in[i];
            }
        }

    }

    public static class SketcherError implements Runnable {
        private Thread t;
        private String threadName;

        SketcherError(String name) {
            threadName = name;
            System.out.println("Creating " + threadName);
        }

        public void run() {
            try {
                System.out.println("Running " + threadName);
                int[] in = new int[] { 0, 1, 2, 3, 4, 5, 6, 7 };
                int[] out = new int[8];

                // >>>>> UNCOMMENT FOR CASE 2 <<<<<
                // System.out.println("=== Execute simple function");
                // Test t = new Test();
                // new TaskSchedule("s1").task("t1", t::test, in, out).streamOut(out).execute();

                // analyze map class
                TransformUDF.mapUserClassName = userClassName;
                TransformUDF.tornadoMapName = "uk/ac/manchester/tornado/examples/MapASMSkeleton";
                ExamineUDF.FlinkClassVisitor flinkVisit = new ExamineUDF.FlinkClassVisitor();
                ClassReader flinkClassReader = new ClassReader(TransformUDF.mapUserClassName);
                flinkClassReader.accept(flinkVisit, 0);

                ExamineUDF.setTypeVariablesMap();

                // patch udf into the appropriate MapASMSkeleton
                String desc = "L" + TransformUDF.mapUserClassName + ";";
                ClassReader readerMap;

                readerMap = new ClassReader("uk.ac.manchester.tornado.examples.MapASMSkeleton");
                ClassWriter writerMap = new ClassWriter(readerMap, ClassWriter.COMPUTE_MAXS);
                writerMap.visitField(Opcodes.ACC_PUBLIC, "udf", desc, null, null).visitEnd();
                TransformUDF.MapClassAdapter adapterMap = new TransformUDF.MapClassAdapter(writerMap);
                readerMap.accept(adapterMap, ClassReader.EXPAND_FRAMES);

                byte[] b = writerMap.toByteArray();
                classLoader cl = new classLoader();
                String classname = "uk.ac.manchester.tornado.examples.MapASMSkeleton";
                Class clazz = cl.defineClass(classname, b);

                MiddleMap md = (MiddleMap) clazz.newInstance();
                TornadoMap msk = new TornadoMap(md);

                System.out.println("=== Execute ASM function");
                TaskSchedule tt = new TaskSchedule("st").task("t0", msk::map, in, out).streamOut(out);
                tt.execute();

                System.out.println(Arrays.toString(out));
                System.out.println("Thread " + threadName + " exiting.");
            } catch (Exception e) {
                System.out.println(e);
            }
        }

        public void start() {
            System.out.println("Starting " + threadName);
            if (t == null) {
                t = new Thread(this, threadName);
                t.start();
            }
        }
    }

    public static class TestTwoThreads {
        public void runTwoThreads() {
            SketcherError thrd1 = new SketcherError("Thread_1");
            SketcherError thrd2 = new SketcherError("Thread_2");
            thrd1.start();
            thrd2.start();
        }
    }

    public static class SingleThread {
        public void runThread() {
            SketcherError thrd1 = new SketcherError("Single_Thread");
            thrd1.start();
        }
    }

    public static void main(String[] args) {
        // run with two threads
        TestTwoThreads twoThreads = new TestTwoThreads();
        twoThreads.runTwoThreads();
        // >>>>> UNCOMMENT FOR CASE 3 <<<<<
        // run with one thread
        // SingleThread singleThread = new SingleThread();
        // singleThread.runThread();

    }

}
