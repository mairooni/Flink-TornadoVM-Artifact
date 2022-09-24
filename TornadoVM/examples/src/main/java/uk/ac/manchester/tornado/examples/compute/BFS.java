/*
 * Copyright (c) 2013-2020, APT Group, Department of Computer Science,
 * The University of Manchester.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package uk.ac.manchester.tornado.examples.compute;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;

import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.annotations.Parallel;
import uk.ac.manchester.tornado.api.common.TornadoDevice;
import uk.ac.manchester.tornado.api.runtime.TornadoRuntime;

/**
 * Parallel Implementation of the BFS: this is based on the Marawacc compiler
 * framework.
 * 
 * @author Juan Fumero
 *
 */
public class BFS {

    private static final boolean BIDIRECTIONAL = false;
    private static final boolean PRINT_SOLUTION = false;
    private static final boolean VALIDATION = true;

    int[] vertices;
    int[] verticesJava;
    int[] adjacencyMatrix;
    int[] modify;
    int[] modifyJava;
    int[] currentDepth;

    public static final boolean SAMPLE = false;

    /**
     * Set to one the connection between node from and node to into the adjacency
     * matrix.
     *
     * @param from
     * @param to
     * @param graph
     * @param N
     */
    public static void connect(int from, int to, int[] graph, int N) {
        if (from != to && (graph[from * N + to] == 0)) {
            graph[from * N + to] = 1;
        }
    }

    /**
     * It builds a simple graph just for showing the example.
     *
     * @param adjacencyMatrix
     * @param numNodes
     */
    public static void initilizeAdjacencyMatrixSimpleGraph(int[] adjacencyMatrix, int numNodes) {
        Arrays.fill(adjacencyMatrix, 0);
        connect(0, 1, adjacencyMatrix, numNodes);
        connect(0, 4, adjacencyMatrix, numNodes);
        connect(1, 2, adjacencyMatrix, numNodes);
        connect(2, 3, adjacencyMatrix, numNodes);
        connect(2, 4, adjacencyMatrix, numNodes);
        connect(3, 4, adjacencyMatrix, numNodes);
    }

    private static int[] generateIntRandomArray(int numNodes) {
        Random r = new Random();
        int bound = r.nextInt(numNodes);
        IntStream streamArray = r.ints(bound, 0, numNodes);
        return streamArray.toArray();
    }

    public static void generateRandomGraph(int[] adjacencyMatrix, int numNodes, int root) {
        Random r = new Random();
        int bound = r.nextInt(numNodes);
        IntStream fromStream = r.ints(bound, 0, numNodes);
        int[] f = fromStream.toArray();
        for (int k = 0; k < f.length; k++) {

            int from = f[k];
            if (k == 0) {
                from = root;
            }

            int[] toArray = generateIntRandomArray(numNodes);

            for (int i = 0; i < toArray.length; i++) {
                connect(from, toArray[i], adjacencyMatrix, numNodes);
            }
        }
    }

    private static void initializeVertices(int numNodes, int[] vertices, int root) {
        for (@Parallel int i = 0; i < numNodes; i++) {
            if (i == root) {
                vertices[i] = 0;
            } else {
                vertices[i] = -1;
            }
        }
    }

    private static void runBFS(int[] vertices, int[] adjacencyMatrix, int numNodes, int[] h_true, int[] currentDepth) {
        for (@Parallel int from = 0; from < numNodes; from++) {
            for (@Parallel int to = 0; to < numNodes; to++) {
                int elementAccess = from * numNodes + to;

                if (adjacencyMatrix[elementAccess] == 1) {
                    int dfirst = vertices[from];
                    int dsecond = vertices[to];
                    if ((currentDepth[0] == dfirst) && (dsecond == -1)) {
                        vertices[to] = dfirst + 1;
                        h_true[0] = 0;
                    }

                    if (BIDIRECTIONAL) {
                        if ((currentDepth[0] == dsecond) && (dfirst == -1)) {
                            vertices[from] = dsecond + 1;
                            h_true[0] = 0;
                        }
                    }
                }
            }
        }
    }

    public boolean validateBFS(int[] vertices, int[] verticesJava) {
        boolean check = true;
        for (int i = 0; i < vertices.length; i++) {
            if (vertices[i] != verticesJava[i]) {
                check = false;
            }
        }
        return check;
    }

    public boolean checkModify(int[] modify, int[] modifyJava) {
        boolean check = true;
        for (int i = 0; i < modify.length; i++) {
            if (modify[i] != modifyJava[i]) {
                check = false;
            }
        }
        return check;
    }

    public void tornadoBFS(int rootNode, int numNodes) throws IOException {

        vertices = new int[numNodes];
        verticesJava = new int[numNodes];
        adjacencyMatrix = new int[numNodes * numNodes];
        boolean validModifyResults = true;

        if (SAMPLE) {
            initilizeAdjacencyMatrixSimpleGraph(adjacencyMatrix, numNodes);
        } else {
            generateRandomGraph(adjacencyMatrix, numNodes, rootNode);
        }

        // Step 1: vertices initialisation
        initializeVertices(numNodes, vertices, rootNode);
        TaskSchedule s0 = new TaskSchedule("s0");
        s0.task("t0", BFS::initializeVertices, numNodes, vertices, rootNode);
        s0.streamOut(vertices).execute();

        // initialization of Java vertices
        initializeVertices(numNodes, verticesJava, rootNode);

        modify = new int[] { 1 };
        Arrays.fill(modify, 1);

        modifyJava = new int[] { 1 };
        Arrays.fill(modifyJava, 1);

        currentDepth = new int[] { 0 };

        TornadoDevice device = TornadoRuntime.getTornadoRuntime().getDefaultDevice();
        TaskSchedule s1 = new TaskSchedule("s1");
        s1.streamIn(vertices, adjacencyMatrix, modify, currentDepth).mapAllTo(device);
        s1.task("t1", BFS::runBFS, vertices, adjacencyMatrix, numNodes, modify, currentDepth);
        s1.streamOut(vertices, modify);

        boolean done = false;

        while (!done) {
            // 2. Parallel BFS
            boolean allDone = true;
            System.out.println("Current Depth: " + currentDepth[0]);
            runBFS(verticesJava, adjacencyMatrix, numNodes, modifyJava, currentDepth);
            s1.execute();
            currentDepth[0]++;

            if (VALIDATION) {
                if (!(validModifyResults = checkModify(modify, modifyJava))) {
                    break;
                }
            }

            for (int i = 0; i < modify.length; i++) {
                if (modify[i] == 0) {
                    allDone &= false;
                    break;
                }
            }

            if (allDone) {
                done = true;
            }
            Arrays.fill(modify, 1);
            Arrays.fill(modifyJava, 1);
        }

        if (PRINT_SOLUTION) {
            System.out.println("Solution: " + Arrays.toString(vertices));
        }

        if (VALIDATION) {
            if (validateBFS(vertices, verticesJava) && validModifyResults) {
                System.out.println("Validation true");
            } else {
                System.out.println("Validation false");
            }
        }
    }

    public static void main(String[] args) throws IOException {
        int size = 10000;
        if (SAMPLE) {
            size = 5;
        }
        new BFS().tornadoBFS(0, size);
    }

}
