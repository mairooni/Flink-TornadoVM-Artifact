import java.io.*;
import java.util.ArrayList;
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
public class Speedups {
    /**
     * Run with arguments:
     * --tornadoFlinkConf &lt;The configuration of Flink-TornadoVM i.e. 1 or 2&gt;
     * --tornadoFlink &lt;The path to end-to-end numbers of Flink-TornadoVM&gt;
     * --flink  &lt;The path to end-to-end numbers of Flink&gt;
     * --usecase &lt;The use case i.e. MM or KM or DFT or IoT or Pi or VAdd&gt;
     * --output &lt;The path to store the results&gt;
     */
    public static void main(String[] args) throws IOException {
        if (args.length == 0 || args.length%2 != 0) {
            System.out.println("Please provide the following inputs: --tornadoFlinkConf --tornadoFlink --flink --usecase");
            return;
        }
        HashMap<String, String> inputs = getArguments(args);

        if (inputs == null || !checkIfValid(inputs)) return;

        ArrayList<Integer> datasets = getDatasets(inputs.get("--usecase"));

        // create file to write results
        File fw = new File(inputs.get("--output") + "/" + inputs.get("--tornadoFlinkConf") + "TM_speedups.csv");
        fw.createNewFile();
        BufferedWriter csvWriter = new BufferedWriter(new FileWriter(fw, true));
        csvWriter.write("Size,SpeedUp Over Flink: N-1 TS-CPU-1,SpeedUp Over Flink: N-1 TS-CPU-2,SpeedUp Over Flink: N-1 TS-CPU-4,SpeedUp Over Flink: N-2 TS-CPU-1,SpeedUp Over Flink: N-2 TS-CPU-2,SpeedUp Over Flink: N-2 TS-CPU-4\n");
        for (Integer size : datasets) {
            // calculate the average of Flink-TornadoVM
            int flinkTornadoAVG = calculateAVG(inputs.get("--tornadoFlink") + "/" + inputs.get("--tornadoFlinkConf") + "/1/" + size + "/" + size + ".txt");
            // calculate the average of Flink configurations
            int flink1AVG = calculateAVG(inputs.get("--flink") + "/1/1/" + size + "/" + size + ".txt");
            int flink2AVG = calculateAVG(inputs.get("--flink") + "/1/2/" + size + "/" + size + ".txt");
            int flink3AVG = calculateAVG(inputs.get("--flink") + "/1/4/" + size + "/" + size + ".txt");
            int flink4AVG = calculateAVG(inputs.get("--flink") + "/2/1/" + size + "/" + size + ".txt");
            int flink5AVG = calculateAVG(inputs.get("--flink") + "/2/2/" + size + "/" + size + ".txt");
            int flink6AVG = calculateAVG(inputs.get("--flink") + "/2/4/" + size + "/" + size + ".txt");
            // calculate speedups
            double flink1Speedup = ((double) flink1AVG) / flinkTornadoAVG;
            double flink2Speedup = ((double) flink2AVG) / flinkTornadoAVG;
            double flink3Speedup = ((double) flink3AVG) / flinkTornadoAVG;
            double flink4Speedup = ((double) flink4AVG) / flinkTornadoAVG;
            double flink5Speedup = ((double) flink5AVG) / flinkTornadoAVG;
            double flink6Speedup = ((double) flink6AVG) / flinkTornadoAVG;
            String line = size + "," + flink1Speedup + "," + flink2Speedup + "," + flink3Speedup + "," + flink4Speedup + "," + flink5Speedup + "," + flink6Speedup;
            csvWriter.write(line+ "\n");
            csvWriter.flush();
        }
        csvWriter.close();

    }

    private static int calculateAVG(String path) {
        int AVG = 0;
        try {
            FileReader fileReader = new FileReader(path);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            int sum = 0, num = 0;
            while ((line = bufferedReader.readLine()) != null) {
                if (line.contains("Job Runtime:")) {
                    String[] strArray = line.split(" ");
                    String time = strArray[strArray.length - 2];
                    sum += Integer.parseInt(time);
                    num++;
                }
            }
            AVG = sum/num;
            return AVG;
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
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
                System.out.println("Please provide the following inputs: --tornadoFlinkConf --tornadoFlink --flink --usecase");
                return null;
            }
        }
        return map;
    }

    private static boolean checkIfValid(HashMap<String, String > args) {
        String[] keys = {"--tornadoFlinkConf", "--tornadoFlink", "--flink", "--usecase"};
        for (String k : keys) {
            if (!args.containsKey(k)) {
                System.out.println("Please provide the following inputs: --tornadoFlinkConf --tornadoFlink --flink --usecase");
                return false;
            }
        }
        return true;
    }

    private static ArrayList<Integer> getDatasets(String usecase) {
        ArrayList<Integer> datasets = new ArrayList<>();
        if (usecase.equals("MM")) {
            datasets.add(128);
            datasets.add(256);
            datasets.add(512);
            datasets.add(1024);
            datasets.add(2048);
            datasets.add(4096);
            return datasets;
        } else if (usecase.equals("MM-FPGA")) {
            datasets.add(128);
            datasets.add(256);
            datasets.add(512);
            return datasets;
        } else if (usecase.equals("KM")) {
            datasets.add(32768);
            datasets.add(65536);
            datasets.add(131072);
            datasets.add(262144);
            datasets.add(524288);
            datasets.add(1048576);
            datasets.add(2097152);
            datasets.add(4194304);
            datasets.add(8388608);
            datasets.add(16777216);
            return datasets;
        } else if (usecase.equals("DFT")) {
            datasets.add(2048);
            datasets.add(4096);
            datasets.add(8192);
            datasets.add(16384);
            datasets.add(32768);
            datasets.add(65536);
            return datasets;
        } else if (usecase.equals("IoT")) {
            datasets.add(346);
            return datasets;
        } else if (usecase.equals("Pi")) {
            datasets.add(1048576);
            datasets.add(2097152);
            datasets.add(4194304);
            datasets.add(8388608);
            datasets.add(16777216);
            return datasets;
        } else if (usecase.equals("VAdd")) {
            datasets.add(1048576);
            datasets.add(2097152);
            datasets.add(4194304);
            datasets.add(8388608);
            datasets.add(16777216);
            return datasets;
        } else {
            System.out.println("ERROR: " + usecase + " is not a valid use case. Please input one of the following: MM, KM, DFT, IoT, Pi, VAdd.");
            return null;
        }
    }

}

