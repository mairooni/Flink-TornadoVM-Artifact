import java.io.*;
import java.util.*;

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
public class BreakdownEvaluation {

    /**
     * Run with arguments:
     * --log &lt;The Flink Task Manager out file that contains the timers&gt;
     * --directory &lt;The directory to store the csv files that have the analysis&gt;
     */
    public static void main (String[] args) {
        FileReader fileReader = null;
        String line;
        Queue<String> times = new LinkedList<String>();
        HashMap<String, String> inputs = getArguments(args);
        if (inputs == null || !checkIfValid(inputs)) return;

        String logFile = inputs.get("--log");
        String directory = inputs.get("--directory");

        try {
            fileReader = new FileReader(logFile);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            while ((line = bufferedReader.readLine()) != null) {
                if (line.contains("*** Function")) {
                    String[] strArray = line.split(" ");
                    String time = strArray[strArray.length - 1];
                    times.add(time);
                }
            }
            bufferedReader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // write to csv
        try {
            // organize breakdown numbers in csv file
            writeCSV(times, directory);
            // calculate the average
            writeAVG(directory);
            // summarize all iterations and categorize
            getBreakdown(directory);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void getBreakdown(String directory) throws IOException {
        File f = new File(directory + "/NumbersFilled-AVG.csv");
        BufferedReader csvReader = new BufferedReader(new FileReader(f));
        String row;
        int map = 0;
        int reduce = 0;
        int marshalling = 0;
        int rest = 0;
        int[] iter = new int[16];
        int [] reg = new int[12];
        while ((row = csvReader.readLine()) != null) {
            if (!row.contains("AVERAGE") && !row.contains("Iteration") && !row.contains("Get") & !row.contains("Predict")) {
                //System.out.println(row);
                if (row.startsWith(",")) {
                    StringTokenizer multiTokenizer = new StringTokenizer(row, ",");
                    int i = 0;
                    while (multiTokenizer.hasMoreTokens()) {
                        String tok = multiTokenizer.nextToken();
                        iter[i] = iter[i] + Integer.parseInt(tok);
                        i++;
                    }
                } else {
                    StringTokenizer multiTokenizer = new StringTokenizer(row, ",");
                    int i = 0;
                    while (multiTokenizer.hasMoreTokens()) {
                        String tok = multiTokenizer.nextToken();
                        reg[i] = Integer.parseInt(tok);
                        i++;
                    }
                }
            }
        }
        csvReader.close();

        // ============ for iterations
        // map is in positions: 4 6 13
        map = iter[4] + iter[7] + iter[13];
        // marshalling 0 and 1
        marshalling = iter[0] + iter[1];
        // reduce 9
        reduce = iter[9];
        // rest 2 3 5 8 9 10 11 12 13
        rest = iter[2] + iter[3] + iter[5] + iter[6] + iter[8] + iter[10] + iter[11] + iter[12] + iter[14] + iter[15];
        // ============ for the simple pipeline
        // map is in positions: 4 9
        map = map + reg[4] + reg[10];
        // marshalling 0 and 1
        marshalling = marshalling + reg[0] + reg[1];
        // reduce 5
        reduce = reduce + reg[6];
        // rest 2 3 6 7 8 10
        rest = rest + reg[2] + reg[3] + reg[5] + reg[7] + reg[8] + reg[9] + reg[11];

        File fw = new File(directory + "/Breakdown-Simplified.csv");
        boolean exists = fw.createNewFile();
        BufferedWriter csvWriter = new BufferedWriter(new FileWriter(fw, exists));
        csvWriter.write("Marshalling,TornadoVM Maps,TornadoVM Reductions, Rest\n");
        csvWriter.write(marshalling + "," + map + "," + reduce + "," + rest + "\n");
        csvWriter.close();
    }

    public static void writeCSV (Queue times, String directory) throws IOException {
        File fw = new File(directory + "/NumbersFilled.csv");
        boolean exists = fw.createNewFile();
        BufferedWriter csvWriter = new BufferedWriter(new FileWriter(fw, exists));
        int run = 1;
        int line = 1;
        while (run <= 10) {
            csvWriter.write("RUN " + run + ",,,,,,,,,,,,,,\n");
            while (line <= 11) {
                String newLine;
                int numOfRows;
                if (line == 11) {
                    String bd = ",,Predict Map,,,,Evaluate Reduce,,,,ComputeMetric Map,,,,,\n" +
                            "Get broadcasted data,Get input data,ASM Skeleton,Set Comp Info,Execution time,ASM Skeleton, Execution Time,Collect PR,ASM Skeleton,Set Comp Info,Execution time,Break into records,,,,\n";
                    csvWriter.write(bd);
                    // write data
                    numOfRows = 12; //11;
                    newLine = "";
                } else {
                    String iter = "Iteration #" + line + ",,,SubUpdate Map,,,,UpdateAccumulatorMap,,,UpdateAccumulator,,,,,Update Map,,\n";
                    csvWriter.write(iter);
                    String bd = ",Get broadcasted data,Get input data,ASM Skeleton,Set Comp Info,Execution time,ASM Skeleton,Collect Info,Map Execution Time,ASM Skeleton,Execution Time,Collect PR,ASM Skeleton,Set Comp Info,Execution time,Change endianess,Break into records\n";
                    csvWriter.write(bd);
                    // write data
                    numOfRows = 16; //14;
                    newLine = ",";
                }
                int count = 16;
                for (int i = 0; i < numOfRows; i++) {
                    newLine = newLine + times.remove();
                    if (count > 0) {
                        newLine = newLine + ",";
                        count--;
                    }
                }
                while (count > 0) {
                    newLine = newLine + ",";
                    count--;
                }
                csvWriter.write(newLine + "\n");
                line++;
            }
            run++;
            line = 1;
        }
        csvWriter.flush();
    }

    public static void writeAVG(String directory) throws IOException {
        File f = new File(directory + "/NumbersFilled.csv");
        BufferedReader csvReader = new BufferedReader(new FileReader(f));
        String row;

        ArrayList<int[]> iteration1 = new ArrayList<>();
        ArrayList<int[]> iteration2 = new ArrayList<>();
        ArrayList<int[]> iteration3 = new ArrayList<>();
        ArrayList<int[]> iteration4 = new ArrayList<>();
        ArrayList<int[]> iteration5 = new ArrayList<>();
        ArrayList<int[]> iteration6 = new ArrayList<>();
        ArrayList<int[]> iteration7 = new ArrayList<>();
        ArrayList<int[]> iteration8 = new ArrayList<>();
        ArrayList<int[]> iteration9 = new ArrayList<>();
        ArrayList<int[]> iteration10 = new ArrayList<>();
        ArrayList<int[]> secondPipeline = new ArrayList<>();

        while ((row = csvReader.readLine()) != null) {
            if (row.contains("Iteration #1") && !row.contains("Iteration #10")) {
                row = csvReader.readLine();
                row = csvReader.readLine();
                int[] it1 = new int[16];
                int i = 0;
                StringTokenizer multiTokenizer = new StringTokenizer(row, ",");
                while (multiTokenizer.hasMoreTokens()) {
                    String tok = multiTokenizer.nextToken();
                    it1[i] = Integer.parseInt(tok);
                    i++;
                }
                iteration1.add(it1);
            } else if (row.contains("Iteration #2")) {
                row = csvReader.readLine();
                row = csvReader.readLine();
                int[] it2 = new int[16]; //17];
                int i = 0;
                StringTokenizer multiTokenizer = new StringTokenizer(row, ",");
                while (multiTokenizer.hasMoreTokens()) {
                    String tok = multiTokenizer.nextToken();
                    it2[i] = Integer.parseInt(tok);
                    i++;
                }
                iteration2.add(it2);
            } else if (row.contains("Iteration #3")) {
                row = csvReader.readLine();
                row = csvReader.readLine();
                int[] it3 = new int[16];
                int i = 0;
                StringTokenizer multiTokenizer = new StringTokenizer(row, ",");
                while (multiTokenizer.hasMoreTokens()) {
                    String tok = multiTokenizer.nextToken();
                    it3[i] = Integer.parseInt(tok);
                    i++;
                }
                iteration3.add(it3);
            } else if (row.contains("Iteration #4")) {
                row = csvReader.readLine();
                row = csvReader.readLine();
                int[] it4 = new int[16];
                int i = 0;
                StringTokenizer multiTokenizer = new StringTokenizer(row, ",");
                while (multiTokenizer.hasMoreTokens()) {
                    String tok = multiTokenizer.nextToken();
                    it4[i] = Integer.parseInt(tok);
                    i++;
                }
                iteration4.add(it4);
            } else if (row.contains("Iteration #5")) {
                row = csvReader.readLine();
                row = csvReader.readLine();
                int[] it5 = new int[16];
                int i = 0;
                StringTokenizer multiTokenizer = new StringTokenizer(row, ",");
                while (multiTokenizer.hasMoreTokens()) {
                    String tok = multiTokenizer.nextToken();
                    it5[i] = Integer.parseInt(tok);
                    i++;
                }
                iteration5.add(it5);
            } else if (row.contains("Iteration #6")) {
                row = csvReader.readLine();
                row = csvReader.readLine();
                int[] it6 = new int[16]; //17];
                int i = 0;
                StringTokenizer multiTokenizer = new StringTokenizer(row, ",");
                while (multiTokenizer.hasMoreTokens()) {
                    String tok = multiTokenizer.nextToken();
                    it6[i] = Integer.parseInt(tok);
                    i++;
                }
                iteration6.add(it6);
            } else if (row.contains("Iteration #7")) {
                row = csvReader.readLine();
                row = csvReader.readLine();
                int[] it7 = new int[16];
                int i = 0;
                StringTokenizer multiTokenizer = new StringTokenizer(row, ",");
                while (multiTokenizer.hasMoreTokens()) {
                    String tok = multiTokenizer.nextToken();
                    it7[i] = Integer.parseInt(tok);
                    i++;
                }
                iteration7.add(it7);
            } else if (row.contains("Iteration #8")) {
                row = csvReader.readLine();
                row = csvReader.readLine();
                int[] it8 = new int[16]; //17];
                int i = 0;
                StringTokenizer multiTokenizer = new StringTokenizer(row, ",");
                while (multiTokenizer.hasMoreTokens()) {
                    String tok = multiTokenizer.nextToken();
                    it8[i] = Integer.parseInt(tok);
                    i++;
                }
                iteration8.add(it8);
            } else if (row.contains("Iteration #9")) {
                row = csvReader.readLine();
                row = csvReader.readLine();
                int[] it9 = new int[16];
                int i = 0;
                StringTokenizer multiTokenizer = new StringTokenizer(row, ",");
                while (multiTokenizer.hasMoreTokens()) {
                    String tok = multiTokenizer.nextToken();
                    it9[i] = Integer.parseInt(tok);
                    i++;
                }
                iteration9.add(it9);
            } else if (row.contains("Iteration #10")) {
                row = csvReader.readLine();
                row = csvReader.readLine();
                int[] it10 = new int[16]; //17];
                int i = 0;
                StringTokenizer multiTokenizer = new StringTokenizer(row, ",");
                while (multiTokenizer.hasMoreTokens()) {
                    String tok = multiTokenizer.nextToken();
                    it10[i] = Integer.parseInt(tok);
                    i++;
                }
                iteration10.add(it10);
            } else if (row.contains("Predict")) {
                row = csvReader.readLine();
                row = csvReader.readLine();
                int[] sec = new int[12];
                int i = 0;
                StringTokenizer multiTokenizer = new StringTokenizer(row, ",");
                while (multiTokenizer.hasMoreTokens()) {
                    String tok = multiTokenizer.nextToken();
                    sec[i] = Integer.parseInt(tok);
                    i++;
                }
                secondPipeline.add(sec);
            }
        }

        csvReader.close();

        // averate for iteration 1
        int [] it1avg = new int[16];
        for (int[] ar : iteration1) {
            for (int i = 0; i < ar.length; i++) {
                it1avg[i] += ar[i];
            }
        }
        for (int i = 0; i < it1avg.length; i++) {
            it1avg[i] = it1avg[i] / 10;
        }

        // averate for iteration 2
        int [] it2avg = new int[16];
        for (int[] ar : iteration2) {
            for (int i = 0; i < ar.length; i++) {
                it2avg[i] += ar[i];
            }
        }
        for (int i = 0; i < it2avg.length; i++) {
            it2avg[i] = it2avg[i] / 10;
        }

        // averate for iteration 3
        int [] it3avg = new int[16];
        for (int[] ar : iteration3) {
            for (int i = 0; i < ar.length; i++) {
                it3avg[i] += ar[i];
            }
        }
        for (int i = 0; i < it3avg.length; i++) {
            it3avg[i] = it3avg[i] / 10;
        }

        // averate for iteration 4
        int [] it4avg = new int[16];
        for (int[] ar : iteration4) {
            for (int i = 0; i < ar.length; i++) {
                it4avg[i] += ar[i];
            }
        }
        for (int i = 0; i < it4avg.length; i++) {
            it4avg[i] = it4avg[i] / 10;
        }

        // averate for iteration 5
        int [] it5avg = new int[16];
        for (int[] ar : iteration5) {
            for (int i = 0; i < ar.length; i++) {
                it5avg[i] += ar[i];
            }
        }
        for (int i = 0; i < it5avg.length; i++) {
            it5avg[i] = it5avg[i] / 10;
        }

        // averate for iteration 6
        int [] it6avg = new int[16];
        for (int[] ar : iteration6) {
            for (int i = 0; i < ar.length; i++) {
                it6avg[i] += ar[i];
            }
        }
        for (int i = 0; i < it6avg.length; i++) {
            it6avg[i] = it6avg[i] / 10;
        }

        // averate for iteration 7
        int [] it7avg = new int[16];
        for (int[] ar : iteration7) {
            for (int i = 0; i < ar.length; i++) {
                it7avg[i] += ar[i];
            }
        }
        for (int i = 0; i < it7avg.length; i++) {
            it7avg[i] = it7avg[i] / 10;
        }

        // averate for iteration 8
        int [] it8avg = new int[16];
        for (int[] ar : iteration8) {
            for (int i = 0; i < ar.length; i++) {
                it8avg[i] += ar[i];
            }
        }
        for (int i = 0; i < it8avg.length; i++) {
            it8avg[i] = it8avg[i] / 10;
        }

        // averate for iteration 9
        int [] it9avg = new int[16];
        for (int[] ar : iteration9) {
            for (int i = 0; i < ar.length; i++) {
                it9avg[i] += ar[i];
            }
        }
        for (int i = 0; i < it9avg.length; i++) {
            it9avg[i] = it9avg[i] / 10;
        }

        // averate for iteration 10
        int [] it10avg = new int[16];
        for (int[] ar : iteration10) {
            for (int i = 0; i < ar.length; i++) {
                it10avg[i] += ar[i];
            }
        }
        for (int i = 0; i < it10avg.length; i++) {
            it10avg[i] = it10avg[i] / 10;
        }

        // averate for second pipeline
        int [] secavg = new int[12];
        for (int[] ar : secondPipeline) {
            for (int i = 0; i < ar.length; i++) {
                secavg[i] += ar[i];
            }
        }
        for (int i = 0; i < secavg.length; i++) {
            secavg[i] = secavg[i] / 10;
        }

        File fw = new File(directory + "/NumbersFilled-AVG.csv");
        boolean exists = fw.createNewFile();
        BufferedWriter csvWriter = new BufferedWriter(new FileWriter(fw, exists));
        File fr = new File(directory + "/NumbersFilled.csv");
        BufferedReader csvReader2 = new BufferedReader(new FileReader(fr));
        int k = 0;
        int[] dat = null;
        while (k < 34) {
            String input = csvReader2.readLine();
            if (input.contains("RUN") || input.contains("Iteration") || input.contains("Get broadcasted data") || input.contains("Predict")) {
                if (input.contains("RUN")) {
                    csvWriter.write("AVERAGE\n");
                } else {
                    csvWriter.write(input + "\n");
                }
                csvWriter.flush();
                if (input.contains("Iteration #1") && !input.contains("Iteration #10")) dat = it1avg;
                else if (input.contains("Iteration #2")) dat = it2avg;
                else if (input.contains("Iteration #3")) dat = it3avg;
                else if (input.contains("Iteration #4")) dat = it4avg;
                else if (input.contains("Iteration #5")) dat = it5avg;
                else if (input.contains("Iteration #6")) dat = it6avg;
                else if (input.contains("Iteration #7")) dat = it7avg;
                else if (input.contains("Iteration #8")) dat = it8avg;
                else if (input.contains("Iteration #9")) dat = it9avg;
                else if (input.contains("Iteration #10")) dat = it10avg;
                else if (input.contains("Predict")) dat = secavg;
            } else {
                StringTokenizer multiTokenizer = new StringTokenizer(input, ",");
                int numOfRows = multiTokenizer.countTokens();
                long count = input.chars().filter(ch -> ch == ',').count();

                String newLine;
                char firstCh = input.charAt(0);
                if (firstCh == ',') {
                    newLine = ",";
                    count--;
                } else {
                    newLine = "";
                }
                for (int i = 0; i < numOfRows; i++) {
                    newLine = newLine + dat[i];
                    if (count > 0) {
                        newLine = newLine + ",";
                        count--;
                    }
                }
                while (count > 0) {
                    newLine = newLine + ",";
                    count--;
                }
                csvWriter.write(newLine + "\n");
                csvWriter.flush();
            }

            k++;
        }

        csvReader2.close();
        csvWriter.close();
    }

    private static HashMap getArguments (String[] args) {
        final HashMap<String, String> map = new HashMap<>(args.length / 2);
        int i = 0;
        while (i < args.length) {
            if (args[i].startsWith("--")) {
                map.put(args[i], args[i + 1]);
                i+=2;
            } else {
                System.out.println("Please provide the following inputs: --log --directory");
                return null;
            }
        }
        return map;
    }

    private static boolean checkIfValid(HashMap<String, String > args) {
        String[] keys = {"--log", "--directory"};
        for (String k : keys) {
            if (!args.containsKey(k)) {
                System.out.println("Please provide the following inputs: --log --directory");
                return false;
            }
        }
        return true;
    }
}
