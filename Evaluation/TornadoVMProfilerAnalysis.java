import java.io.*;
import java.math.BigInteger;
import java.util.HashMap;

/**
 * Copyright 2022 Mary Xekalaki
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
public class TornadoVMProfilerAnalysis {

    /**
     * Run with arguments:
     * --profiler &lt;The output of the TornadoVM profiler&gt;
     * --directory &lt;The directory to store the csv files that have the analysis&gt;
     * --size &lt;The size of the dataset&gt;
     */
    public static void main(String[] args) {
        try {
            HashMap<String, String> inputs = getArguments(args);
            if (inputs == null || !checkIfValid(inputs)) return;

            String profilerFile = inputs.get("--profiler");
            String directory = inputs.get("--directory");
            String size = inputs.get("--size");

            FileReader fileReader = new FileReader(profilerFile);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            HashMap<String, BigInteger> s0 = new HashMap<>();
            HashMap<String, BigInteger> s1 = new HashMap<>();
            HashMap<String, BigInteger> s2 = new HashMap<>();
            HashMap<String, BigInteger> s3 = new HashMap<>();
            HashMap<String, BigInteger> s4 = new HashMap<>();
            HashMap<String, BigInteger> s5 = new HashMap<>();
            HashMap<String, BigInteger> s6 = new HashMap<>();
            while ((line = bufferedReader.readLine()) != null) {
                int num = 0;
                if (line.contains("\"s0\"")) {
                    while (num < 11) {
                        line = bufferedReader.readLine();
                        String[] strArray = line.split(":");
                        String s = strArray[1].replace(",", "");
                        String str2 = s.replaceAll("\"", "");
                        String str3 = str2.replace(" ", "");
                        BigInteger t = new BigInteger(str3);
                        if (s0.containsKey(strArray[0])) {
                            BigInteger curr = s0.get(strArray[0]);
                            BigInteger ncurr = curr.add(t);
                            s0.replace(strArray[0], ncurr);
                        } else {
                            s0.put(strArray[0], t);
                        }
                        num++;
                    }
                } else if (line.contains("\"s1\"")) {
                    while (num < 11) {
                        line = bufferedReader.readLine();
                        String[] strArray = line.split(":");
                        String s = strArray[1].replace(",", "");
                        String str2 = s.replaceAll("\"", "");
                        String str3 = str2.replace(" ", "");
                        BigInteger t = new BigInteger(str3);
                        if (s1.containsKey(strArray[0])) {
                            BigInteger curr = s1.get(strArray[0]);
                            BigInteger ncurr = curr.add(t);
                            s1.replace(strArray[0], ncurr);
                        } else {
                            s1.put(strArray[0], t);
                        }
                        num++;
                    }
                } else if (line.contains("\"s2\"")) {
                    while (num < 11) {
                        line = bufferedReader.readLine();
                        String[] strArray = line.split(":");
                        String s = strArray[1].replace(",", "");
                        String str2 = s.replaceAll("\"", "");
                        String str3 = str2.replace(" ", "");
                        BigInteger t = new BigInteger(str3);
                        if (s2.containsKey(strArray[0])) {
                            BigInteger curr = s2.get(strArray[0]);
                            BigInteger ncurr = curr.add(t);
                            s2.replace(strArray[0], ncurr);
                        } else {
                            s2.put(strArray[0], t);
                        }
                        num++;
                    }
                } else if (line.contains("\"s3\"")) {
                    while (num < 11) {
                        line = bufferedReader.readLine();
                        String[] strArray = line.split(":");
                        String s = strArray[1].replace(",", "");
                        String str2 = s.replaceAll("\"", "");
                        String str3 = str2.replace(" ", "");
                        BigInteger t = new BigInteger(str3);
                        if (s3.containsKey(strArray[0])) {
                            BigInteger curr = s3.get(strArray[0]);
                            BigInteger ncurr = curr.add(t);
                            s3.replace(strArray[0], ncurr);
                        } else {
                            s3.put(strArray[0], t);
                        }
                        num++;
                    }
                } else if (line.contains("\"s4\"")) {
                    while (num < 11) {
                        line = bufferedReader.readLine();
                        String[] strArray = line.split(":");
                        String s = strArray[1].replace(",", "");
                        String str2 = s.replaceAll("\"", "");
                        String str3 = str2.replace(" ", "");
                        BigInteger t = new BigInteger(str3);
                        if (s4.containsKey(strArray[0])) {
                            BigInteger curr = s4.get(strArray[0]);
                            BigInteger ncurr = curr.add(t);
                            s4.replace(strArray[0], ncurr);
                        } else {
                            s4.put(strArray[0], t);
                        }
                        num++;
                    }
                } else if (line.contains("\"s5\"")) {
                    while (num < 11) {
                        line = bufferedReader.readLine();
                        String[] strArray = line.split(":");
                        String s = strArray[1].replace(",", "");
                        String str2 = s.replaceAll("\"", "");
                        String str3 = str2.replace(" ", "");
                        BigInteger t = new BigInteger(str3);
                        if (s5.containsKey(strArray[0])) {
                            BigInteger curr = s5.get(strArray[0]);
                            BigInteger ncurr = curr.add(t);
                            s5.replace(strArray[0], ncurr);
                        } else {
                            s5.put(strArray[0], t);
                        }
                        num++;
                    }
                } else if (line.contains("\"s6\"")) {
                    while (num < 11) {
                        line = bufferedReader.readLine();
                        String[] strArray = line.split(":");
                        String s = strArray[1].replace(",", "");
                        String str2 = s.replaceAll("\"", "");
                        String str3 = str2.replace(" ", "");
                        BigInteger t = new BigInteger(str3);
                        if (s6.containsKey(strArray[0])) {
                            BigInteger curr = s6.get(strArray[0]);
                            BigInteger ncurr = curr.add(t);
                            s6.replace(strArray[0], ncurr);
                        } else {
                            s6.put(strArray[0], t);
                        }
                        num++;
                    }
                }
            }
            BigInteger copyin = new BigInteger("0");
            BigInteger copyout = new BigInteger("0");
            BigInteger tasksch= new BigInteger("0");
            BigInteger kernel = new BigInteger("0");
            //System.out.println("s0:");
            for (String k : s0.keySet()) {
                BigInteger ms = s0.get(k).divide(new BigInteger("10"));
                //BigInteger ms = div.divide(new BigInteger("1000000"));
                //BigInteger ms = s0.get(k);
                //System.out.println(k + " - " + ms);
                if (k.contains("COPY_OUT_TIME")) {
                    copyout = copyout.add(ms);
                } else if (k.contains("COPY_IN_TIME")) {
                    copyin = copyin.add(ms);
                } else if (k.contains("TOTAL_TASK_SCHEDULE_TIME")) {
                    tasksch = tasksch.add(ms);
                } else if (k.contains("TOTAL_KERNEL_TIME")) {
                    kernel = kernel.add(ms);
                }
            }
            //System.out.println("s1:");
            for (String k : s1.keySet()) {
                BigInteger div = s1.get(k).divide(new BigInteger("10"));
                BigInteger ms = div.divide(new BigInteger("1000000"));
                //System.out.println(k + " - " + ms);
                if (k.contains("COPY_OUT_TIME")) {
                    copyout = copyout.add(ms);
                } else if (k.contains("COPY_IN_TIME")) {
                    copyin = copyin.add(ms);
                } else if (k.contains("TOTAL_TASK_SCHEDULE_TIME")) {
                    tasksch = tasksch.add(ms);
                } else if (k.contains("TOTAL_KERNEL_TIME")) {
                    kernel = kernel.add(ms);
                }
            }
            //System.out.println("s2:");
            for (String k : s2.keySet()) {
                BigInteger div = s2.get(k).divide(new BigInteger("10"));
                BigInteger ms = div.divide(new BigInteger("1000000"));
                //System.out.println(k + " - " + ms);
                if (k.contains("COPY_OUT_TIME")) {
                    copyout = copyout.add(ms);
                } else if (k.contains("COPY_IN_TIME")) {
                    copyin = copyin.add(ms);
                } else if (k.contains("TOTAL_TASK_SCHEDULE_TIME")) {
                    tasksch = tasksch.add(ms);
                } else if (k.contains("TOTAL_KERNEL_TIME")) {
                    kernel = kernel.add(ms);
                }
            }
            //System.out.println("s3:");
            for (String k : s3.keySet()) {
                BigInteger div = s3.get(k).divide(new BigInteger("10"));
                BigInteger ms = div.divide(new BigInteger("1000000"));
                //System.out.println(k + " - " + ms);
                if (k.contains("COPY_OUT_TIME")) {
                    copyout = copyout.add(ms);
                } else if (k.contains("COPY_IN_TIME")) {
                    copyin = copyin.add(ms);
                } else if (k.contains("TOTAL_TASK_SCHEDULE_TIME")) {
                    tasksch = tasksch.add(ms);
                } else if (k.contains("TOTAL_KERNEL_TIME")) {
                    kernel = kernel.add(ms);
                }
            }
            //System.out.println("s4:");
            for (String k : s4.keySet()) {
                BigInteger div = s4.get(k).divide(new BigInteger("10"));
                BigInteger ms = div.divide(new BigInteger("1000000"));
                //System.out.println(k + " - " + ms);
                if (k.contains("COPY_OUT_TIME")) {
                    copyout = copyout.add(ms);
                } else if (k.contains("COPY_IN_TIME")) {
                    copyin = copyin.add(ms);
                } else if (k.contains("TOTAL_TASK_SCHEDULE_TIME")) {
                    tasksch = tasksch.add(ms);
                } else if (k.contains("TOTAL_KERNEL_TIME")) {
                    kernel = kernel.add(ms);
                }
            }
            //System.out.println("s5:");
            for (String k : s5.keySet()) {
                BigInteger div = s5.get(k).divide(new BigInteger("10"));
                BigInteger ms = div.divide(new BigInteger("1000000"));
                //System.out.println(k + " - " + ms);
                if (k.contains("COPY_OUT_TIME")) {
                    copyout = copyout.add(ms);
                } else if (k.contains("COPY_IN_TIME")) {
                    copyin = copyin.add(ms);
                } else if (k.contains("TOTAL_TASK_SCHEDULE_TIME")) {
                    tasksch = tasksch.add(ms);
                } else if (k.contains("TOTAL_KERNEL_TIME")) {
                    kernel = kernel.add(ms);
                }
            }
            //System.out.println("s6:");
            for (String k : s6.keySet()) {
                BigInteger div = s6.get(k).divide(new BigInteger("10"));
                BigInteger ms = div.divide(new BigInteger("1000000"));
                //System.out.println(k + " - " + ms);
                if (k.contains("COPY_OUT_TIME")) {
                    copyout = copyout.add(ms);
                } else if (k.contains("COPY_IN_TIME")) {
                    copyin = copyin.add(ms);
                } else if (k.contains("TOTAL_TASK_SCHEDULE_TIME")) {
                    tasksch = tasksch.add(ms);
                } else if (k.contains("TOTAL_KERNEL_TIME")) {
                    kernel = kernel.add(ms);
                }
            }
            File fw = new File(directory + "/TornadoProfilerAnalysis-" + size + ".csv");
            boolean exists = fw.createNewFile();
            BufferedWriter csvWriter = new BufferedWriter(new FileWriter(fw, exists));
            csvWriter.write("TOTAL-COPY-IN,TOTAL-COPY-OUT,TOTAL-TASK-SCHEDULE,TOTAL-KERNEL\n");
            csvWriter.write(copyin + "," + copyout + "," + tasksch + "," + kernel + "\n");
            csvWriter.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static HashMap getArguments (String[] args) {
        final HashMap<String, String> map = new HashMap<>(args.length / 2);
        int i = 0;
        while (i < args.length) {
            if (args[i].startsWith("--")) {
                map.put(args[i], args[i + 1]);
                i+=2;
            } else {
                System.out.println("Please provide the following inputs: --profiler --directory --size");
                return null;
            }
        }
        return map;
    }

    private static boolean checkIfValid(HashMap<String, String > args) {
        String[] keys = {"--profiler", "--directory", "--size"};
        for (String k : keys) {
            if (!args.containsKey(k)) {
                System.out.println("Please provide the following inputs: --profiler --directory --size");
                return false;
            }
        }
        return true;
    }
}
