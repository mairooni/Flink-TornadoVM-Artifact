/*
 * Copyright (c) 2020, APT Group, Department of Computer Science,
 * School of Engineering, The University of Manchester. All rights reserved.
 * Copyright (c) 2018, 2020, APT Group, Department of Computer Science,
 * The University of Manchester. All rights reserved.
 * Copyright (c) 2009, 2017, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 */
package uk.ac.manchester.tornado.runtime.graal.phases;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.HashMap;

import org.graalvm.compiler.graph.Node;
import org.graalvm.compiler.graph.iterators.NodeIterable;
import org.graalvm.compiler.nodes.*;
import org.graalvm.compiler.nodes.calc.*;
import org.graalvm.compiler.nodes.extended.BoxNode;
import org.graalvm.compiler.nodes.extended.UnboxNode;
import org.graalvm.compiler.nodes.java.*;
import org.graalvm.compiler.phases.BasePhase;

import uk.ac.manchester.tornado.api.annotations.Reduce;
import uk.ac.manchester.tornado.api.exceptions.TornadoRuntimeException;
import uk.ac.manchester.tornado.api.flink.FlinkCompilerInfo;
import uk.ac.manchester.tornado.runtime.FlinkCompilerInfoIntermediate;
import uk.ac.manchester.tornado.runtime.graal.nodes.*;

public class TornadoReduceReplacement extends BasePhase<TornadoSketchTierContext> {

    public static HashMap<StoreFieldNode, ValueNode> extraOperation = new HashMap<>();
    public static HashMap<StoreFieldNode, Boolean> reductionField = new HashMap<>();

    private boolean isAvg = false;

    private Node accumAVG = null;
    // public HashMap<Integer, ValueNode> accum = new HashMap<>();

    @Override
    protected void run(StructuredGraph graph, TornadoSketchTierContext context) {
        findParametersWithReduceAnnotations(graph);

        // TODO: Pending, if it is local variable
    }

    /**
     * It checks if there is a reduction in the IR. For now it checks simple
     * reductions. It assumes that the value to store is a binary arithmetic and
     * then load index. As soon as we discover more cases, new nodes should be
     * inspected here.
     *
     * <p>
     * Cover all the cases here as soon as we discover more reductions use-cases.
     * </p>
     *
     * @param arrayToStore
     *            Array to store
     * @param indexToStore
     *            Index used in the store array
     * @param currentNode
     *            Current node to be inspected
     * @return boolean
     */
    private boolean recursiveCheck(ValueNode arrayToStore, ValueNode indexToStore, ValueNode currentNode) {
        boolean isReduction = false;
        // System.out.println(".> current node: " + currentNode);
        if (currentNode instanceof FloatDivNode) {
            // System.out.println(".> Div node!! " + currentNode);
            // check if it is an average calculation
            // we will assume that is the pattern is / + * and at least one input is P(2) we
            // have an avg
            for (AddNode add : currentNode.inputs().filter(AddNode.class)) {
                // System.out.println(".> add node " + add);
                for (Node adIn : add.inputs()) {
                    // System.out.println(".> Add input: " + adIn);
                    if (adIn instanceof MulNode) {
                        MulNode mul = (MulNode) adIn;
                        // System.out.println(".> mul node " + mul);
                        for (Node mulIn : mul.inputs()) {
                            // System.out.println(".> input unbox " + mulIn);
                            if (mulIn instanceof UnboxNode) {
                                Node in = mulIn.inputs().first();
                                if (in instanceof PiNode) {
                                    PiNode p = in.inputs().filter(PiNode.class).first();
                                    if (p.inputs().filter(LoadFieldNode.class).isNotEmpty()) {
                                        LoadFieldNode lf = p.inputs().filter(LoadFieldNode.class).first();
                                        LoadIndexedNode ld = lf.inputs().filter(LoadIndexedNode.class).first();
                                        if (ld != null) {
                                            if (ld.array() == arrayToStore && ld.index() == indexToStore) {
                                                isAvg = true;
                                                isReduction = true;
                                                accumAVG = ld;
                                            }
                                        }
                                    } else {
                                        isReduction = false;
                                    }
                                }
                            } else if (mulIn instanceof FloatConvertNode) {
                                Node fin = mulIn.inputs().first();
                                if (fin instanceof UnboxNode) {
                                    Node in = fin.inputs().first();
                                    if (in instanceof PiNode) {
                                        PiNode p = in.inputs().filter(PiNode.class).first();
                                        if (p.inputs().filter(LoadFieldNode.class).isNotEmpty()) {
                                            LoadFieldNode lf = p.inputs().filter(LoadFieldNode.class).first();
                                            LoadIndexedNode ld = lf.inputs().filter(LoadIndexedNode.class).first();
                                            if (ld != null) {
                                                if (ld.array() == arrayToStore && ld.index() == indexToStore) {
                                                    isAvg = true;
                                                    isReduction = true;
                                                    accumAVG = ld;
                                                }
                                            }
                                        } else {
                                            isReduction = false;
                                        }
                                    }
                                }

                            }
                        }
                    }
                }
            }
        } else if (currentNode instanceof BinaryNode) {
            BinaryNode value = (BinaryNode) currentNode;
            ValueNode x = value.getX();
            isReduction = recursiveCheck(arrayToStore, indexToStore, x);
            if (!isReduction) {
                ValueNode y = value.getY();
                return recursiveCheck(arrayToStore, indexToStore, y);
            }
        } else if (currentNode instanceof LoadIndexedNode) {
            LoadIndexedNode loadNode = (LoadIndexedNode) currentNode;
            if (loadNode.array() == arrayToStore && loadNode.index() == indexToStore) {
                isReduction = true;
            }
        } else if (currentNode instanceof BoxNode) {
            // box nodes have one input
            Node in = currentNode.inputs().first();
            if (in instanceof ValueNode) {
                return recursiveCheck(arrayToStore, indexToStore, (ValueNode) in);
            } else {
                isReduction = false;
            }
        } else if (currentNode instanceof UnboxNode) {
            // unbox nodes have one input
            Node in = currentNode.inputs().first();
            if (in instanceof PiNode) {
                PiNode p = in.inputs().filter(PiNode.class).first();
                if (p.inputs().filter(LoadFieldNode.class).isNotEmpty()) {
                    LoadFieldNode lf = p.inputs().filter(LoadFieldNode.class).first();
                    LoadIndexedNode ld = lf.inputs().filter(LoadIndexedNode.class).first();
                    if (ld != null) {
                        return recursiveCheck(arrayToStore, indexToStore, ld);
                    } else {
                        FlinkCompilerInfo fcomp = FlinkCompilerInfoIntermediate.getFlinkCompilerInfo();
                        if (fcomp != null) {
                            if (fcomp.getNestedTuples()) {
                                // if nested tuples, investigate further
                                PiNode p2 = lf.inputs().filter(PiNode.class).first();
                                if (p2.inputs().filter(LoadFieldNode.class).isNotEmpty()) {
                                    LoadFieldNode lf2 = p2.inputs().filter(LoadFieldNode.class).first();
                                    LoadIndexedNode ld2 = lf2.inputs().filter(LoadIndexedNode.class).first();
                                    if (ld2 != null) {
                                        return recursiveCheck(arrayToStore, indexToStore, ld2);
                                    } else {
                                        isReduction = false;
                                    }
                                } else {
                                    isReduction = false;
                                }
                            } else {
                                isReduction = false;
                            }
                        } else {
                            isReduction = false;
                        }
                    }
                } else {
                    isReduction = false;
                }
            } else {
                // examine more cases
                isReduction = false;
            }
        }
        return isReduction;
    }

    private boolean checkIfReduction(StoreIndexedNode store) {
        ValueNode arrayToStore = store.array();
        ValueNode indexToStore = store.index();
        ValueNode valueToStore = store.value();
        return recursiveCheck(arrayToStore, indexToStore, valueToStore);
    }

    private boolean checkIfReduction(BoxNode b, ValueNode arrayToStore, ValueNode indexToStore) {
        return recursiveCheck(arrayToStore, indexToStore, b);
    }

    private boolean checkIfVarIsInLoop(StoreIndexedNode store) {
        Node node = store.predecessor();
        boolean hasPred = true;
        while (hasPred) {
            if (node instanceof LoopBeginNode) {
                return true;
            } else if (node instanceof StartNode) {
                hasPred = false;
            } else if (node instanceof MergeNode) {
                MergeNode merge = (MergeNode) node;
                EndNode endNode = merge.forwardEndAt(0);
                node = endNode.predecessor();
            } else {
                node = node.predecessor();
            }
        }
        return false;
    }

    private boolean checkIfVarIsInLoop(StoreFieldNode store) {
        Node node = store.predecessor();
        boolean hasPred = true;
        while (hasPred) {
            if (node instanceof LoopBeginNode) {
                return true;
            } else if (node instanceof StartNode) {
                hasPred = false;
            } else if (node instanceof MergeNode) {
                MergeNode merge = (MergeNode) node;
                EndNode endNode = merge.forwardEndAt(0);
                node = endNode.predecessor();
            } else {
                node = node.predecessor();
            }
        }
        return false;
    }

    private static class ReductionMetadataNode {
        private final ValueNode value;
        private final ValueNode accumulator;
        private final ValueNode inputArray;
        private final ValueNode startNode;

        ReductionMetadataNode(ValueNode value, ValueNode accumulator, ValueNode inputArray, ValueNode startNode) {
            super();
            this.value = value;
            this.accumulator = accumulator;
            this.inputArray = inputArray;
            this.startNode = startNode;
        }
    }

    private ValueNode obtainInputArray(ValueNode currentNode, ValueNode outputArray) {
        ValueNode array = null;
        if (currentNode instanceof StoreIndexedNode) {
            StoreIndexedNode store = (StoreIndexedNode) currentNode;
            return obtainInputArray(store.value(), store.array());
        } else if (currentNode instanceof StoreFieldNode) {
            // System.out.println("StoreField: " + currentNode);
            StoreFieldNode stf = (StoreFieldNode) currentNode;
            // System.out.println("call with value " + stf.value());
            return obtainInputArray(stf.value(), outputArray);
        } else if (currentNode instanceof BinaryArithmeticNode) {
            BinaryArithmeticNode<?> value = (BinaryArithmeticNode<?>) currentNode;
            array = obtainInputArray(value.getX(), outputArray);
            if (array == null) {
                array = obtainInputArray(value.getY(), outputArray);
            }
        } else if (currentNode instanceof BinaryNode) {
            if (currentNode instanceof MarkIntrinsicsNode) {
                array = obtainInputArray(((BinaryNode) currentNode).getX(), outputArray);
                if (array == null) {
                    array = obtainInputArray(((BinaryNode) currentNode).getY(), outputArray);
                }
            }
        } else if (currentNode instanceof LoadIndexedNode) {
            LoadIndexedNode loadNode = (LoadIndexedNode) currentNode;
            if (loadNode.array() != outputArray) {
                array = loadNode.array();
            }
        } else if (currentNode instanceof BoxNode) {
            // System.out.println("box node " + currentNode);
            Node in = currentNode.inputs().first();
            if (in instanceof ValueNode) {
                return obtainInputArray((ValueNode) in, outputArray);
            }
        } else if (currentNode instanceof UnboxNode) {
            Node in = currentNode.inputs().first();
            // System.out.println("Unbox " + currentNode);
            if (in instanceof PiNode) {
                PiNode p = in.inputs().filter(PiNode.class).first();
                // System.out.println("Pi in " + currentNode);
                if (p.inputs().filter(LoadFieldNode.class).isNotEmpty()) {
                    LoadFieldNode lf = p.inputs().filter(LoadFieldNode.class).first();
                    // System.out.println("lf " + lf);
                    LoadIndexedNode ld = null;
                    if (lf.inputs().filter(LoadIndexedNode.class).isEmpty()) {
                        // System.out.println("> lf.inputs has loadindex");
                        if (lf.inputs().filter(PiNode.class).isNotEmpty()) {
                            PiNode pp = lf.inputs().filter(PiNode.class).first();
                            // System.out.println("> Pi node " + pp);
                            if (pp.inputs().filter(LoadFieldNode.class).isNotEmpty()) {
                                LoadFieldNode lfp = pp.inputs().filter(LoadFieldNode.class).first();
                                // System.out.println(">lfp " + lfp);
                                if (lfp.inputs().filter(LoadIndexedNode.class).isNotEmpty()) {
                                    // System.out.println(">lfp has ld input ");
                                    ld = lfp.inputs().filter(LoadIndexedNode.class).first();
                                } else {
                                    // System.out.println(">lfp DOES NOT have ld input ");
                                }
                            }
                        }
                    } else {
                        // System.out.println("> lf.inputs doesnot loadindex");
                        ld = lf.inputs().filter(LoadIndexedNode.class).first();
                    }
                    // System.out.println("ld " + ld);
                    return obtainInputArray(ld, outputArray);
                } else if (p.inputs().filter(PiNode.class).isNotEmpty()) {
                    PiNode pp = p.inputs().filter(PiNode.class).first();
                    // System.out.println("Pi pp " + pp);
                    if (pp.inputs().filter(LoadFieldNode.class).isNotEmpty()) {
                        LoadFieldNode lf = pp.inputs().filter(LoadFieldNode.class).first();
                        // System.out.println("lf " + lf);
                        LoadIndexedNode ld = null;
                        if (lf.inputs().filter(LoadIndexedNode.class).isEmpty()) {
                            // System.out.println("lf no ld input ");
                            if (lf.inputs().filter(PiNode.class).isNotEmpty()) {
                                PiNode nestedPi = lf.inputs().filter(PiNode.class).first();
                                // System.out.println("Pi nested " + nestedPi);
                                LoadFieldNode nestedlf = nestedPi.inputs().filter(LoadFieldNode.class).first();
                                // System.out.println("nested lf " + nestedlf);
                                ld = nestedlf.inputs().filter(LoadIndexedNode.class).first();
                                // System.out.println("ld " + ld);
                            }
                        } else {
                            ld = lf.inputs().filter(LoadIndexedNode.class).first();
                        }
                        return obtainInputArray(ld, outputArray);
                    }
                }
            }
        }
        return array;
    }

    private ReductionMetadataNode createReductionNode(StructuredGraph graph, StoreFieldNode store, ValueNode inputArray, ValueNode startNode) throws RuntimeException {
        ValueNode value;
        ValueNode accumulator;

        ValueNode storeValue = store.value();
        // System.out.println("StoreValue is : " + storeValue);
        if (storeValue instanceof AddNode) {
            AddNode addNode = (AddNode) store.value();
            final TornadoReduceAddNode atomicAdd = graph.addOrUnique(new TornadoReduceAddNode(addNode.getX(), addNode.getY()));
            accumulator = addNode.getY();
            value = atomicAdd;
            addNode.safeDelete();
        } else if (storeValue instanceof MulNode) {
            MulNode mulNode = (MulNode) store.value();
            final TornadoReduceMulNode atomicMultiplication = graph.addOrUnique(new TornadoReduceMulNode(mulNode.getX(), mulNode.getY()));
            accumulator = mulNode.getX();
            value = atomicMultiplication;
            mulNode.safeDelete();
        } else if (storeValue instanceof SubNode) {
            SubNode subNode = (SubNode) store.value();
            final TornadoReduceSubNode atomicSub = graph.addOrUnique(new TornadoReduceSubNode(subNode.getX(), subNode.getY()));
            accumulator = subNode.getX();
            value = atomicSub;
            subNode.safeDelete();
        } else if (storeValue instanceof BinaryNode) {

            // We need to compare with the name because it is loaded from inner core
            // (tornado-driver).
            if (storeValue instanceof MarkFloatingPointIntrinsicsNode || storeValue instanceof MarkIntIntrinsicNode) {
                accumulator = ((BinaryNode) storeValue).getY();
                // TODO: Control getX case
            } else {
                // For any other binary node
                // if it is a builtin, we apply the general case
                accumulator = storeValue;
            }
            value = storeValue;
        } else if (storeValue instanceof BoxNode) {
            ValueNode bvalue = ((BoxNode) storeValue).getValue();
            // System.out.println("box's value is " + bvalue);
            // System.out.println("is it average? " + isAvg);
            if (isAvg) {
                // locate unbox nodes
                ArrayList<UnboxNode> inputs = new ArrayList<>();
                ArrayList<Node> toBeDeleted = new ArrayList<>();
                locateUnboxNodes(bvalue, inputs, toBeDeleted);

                TornadoReduceAverageNode tornadoagv = graph
                        .addOrUnique(new TornadoReduceAverageNode(inputs.get(0), inputs.get(1), inputs.get(2), inputs.get(3), inputs.get(4), inputs.get(5), accumAVG));
                accumulator = (ValueNode) accumAVG;
                value = tornadoagv;
                // System.out.println("> hey set avg accum to " + accumAVG);
                storeValue.replaceFirstInput(bvalue, tornadoagv);
                for (Node n : toBeDeleted) {
                    n.safeDelete();
                }
                isAvg = false;
            } else {
                if (bvalue instanceof AddNode) {
                    AddNode addNode = (AddNode) bvalue;
                    final TornadoReduceAddNode atomicAdd = graph.addOrUnique(new TornadoReduceAddNode(addNode.getX(), addNode.getY()));
                    accumulator = addNode.getY();
                    // System.out.println("> Heeey! Accum of add : " + accumulator);
                    value = atomicAdd;
                    storeValue.replaceFirstInput(addNode, atomicAdd);
                    addNode.safeDelete();
                } else if (bvalue instanceof MulNode) {
                    MulNode mulNode = (MulNode) bvalue;
                    final TornadoReduceMulNode atomicMultiplication = graph.addOrUnique(new TornadoReduceMulNode(mulNode.getX(), mulNode.getY()));
                    accumulator = mulNode.getX();
                    value = atomicMultiplication;
                    bvalue.replaceFirstInput(mulNode, atomicMultiplication);
                    mulNode.safeDelete();
                } else if (bvalue instanceof SubNode) {
                    SubNode subNode = (SubNode) bvalue;
                    final TornadoReduceSubNode atomicSub = graph.addOrUnique(new TornadoReduceSubNode(subNode.getX(), subNode.getY()));
                    accumulator = subNode.getX();
                    value = atomicSub;
                    bvalue.replaceFirstInput(subNode, atomicSub);
                    subNode.safeDelete();
                } else if (bvalue instanceof BinaryNode) {
                    if (bvalue instanceof MarkFloatingPointIntrinsicsNode || bvalue instanceof MarkIntIntrinsicNode) {
                        accumulator = ((BinaryNode) bvalue).getY();
                        // TODO: Control getX case
                    } else {
                        // For any other binary node
                        // if it is a builtin, we apply the general case
                        // System.out.println(">! We should be here!!");
                        accumulator = bvalue;
                    }
                    value = bvalue;
                } else {
                    throw new TornadoRuntimeException("\n\n[NODE REDUCTION NOT SUPPORTED] Node : " + bvalue + " not supported yet.");
                }
            }
        } else {
            throw new TornadoRuntimeException("\n\n[NODE REDUCTION NOT SUPPORTED] Node : " + store.value() + " not supported yet.");
        }
        // String field = store.field().toString();
        // int fieldNumber =
        // Integer.parseInt(Character.toString(field.substring(field.lastIndexOf(".") +
        // 1).charAt(1)));
        // accum.put(fieldNumber, accumulator);
        return new ReductionMetadataNode(value, accumulator, inputArray, startNode);
    }

    private ReductionMetadataNode createReductionNode(StructuredGraph graph, StoreIndexedNode store, ValueNode inputArray, ValueNode startNode) throws RuntimeException {
        ValueNode value;
        ValueNode accumulator;

        ValueNode storeValue = store.value();

        if (storeValue instanceof AddNode) {
            AddNode addNode = (AddNode) store.value();
            final TornadoReduceAddNode atomicAdd = graph.addOrUnique(new TornadoReduceAddNode(addNode.getX(), addNode.getY()));
            accumulator = addNode.getY();
            value = atomicAdd;
            addNode.safeDelete();
        } else if (storeValue instanceof MulNode) {
            MulNode mulNode = (MulNode) store.value();
            final TornadoReduceMulNode atomicMultiplication = graph.addOrUnique(new TornadoReduceMulNode(mulNode.getX(), mulNode.getY()));
            accumulator = mulNode.getX();
            value = atomicMultiplication;
            mulNode.safeDelete();
        } else if (storeValue instanceof SubNode) {
            SubNode subNode = (SubNode) store.value();
            final TornadoReduceSubNode atomicSub = graph.addOrUnique(new TornadoReduceSubNode(subNode.getX(), subNode.getY()));
            accumulator = subNode.getX();
            value = atomicSub;
            subNode.safeDelete();
        } else if (storeValue instanceof BinaryNode) {

            // We need to compare with the name because it is loaded from inner core
            // (tornado-driver).
            if (storeValue instanceof MarkFloatingPointIntrinsicsNode || storeValue instanceof MarkIntIntrinsicNode) {
                accumulator = ((BinaryNode) storeValue).getY();
                // TODO: Control getX case
            } else {
                // For any other binary node
                // if it is a builtin, we apply the general case
                accumulator = storeValue;
            }
            value = storeValue;
        } else {
            throw new TornadoRuntimeException("\n\n[NODE REDUCTION NOT SUPPORTED] Node : " + store.value() + " not supported yet.");
        }

        return new ReductionMetadataNode(value, accumulator, inputArray, startNode);
    }

    /**
     * Final Node Replacement
     */
    private void performNodeReplacementTuple(StructuredGraph graph, StoreIndexedNode store, StoreFieldNode stf, Node predecessor, ReductionMetadataNode reductionNode) {
        // System.out.println("----- StoreIndexed: " + store + " - storeField: " + stf);
        if (store.isDeleted()) {
            // store has already been replaced
            return;
        }
        ValueNode value = reductionNode.value;
        ValueNode accumulator = reductionNode.accumulator;
        ValueNode inputArray = reductionNode.inputArray;
        ValueNode startNode = reductionNode.startNode;
        // System.out.println("----- value " + value + " - accumulator " + accumulator +
        // " - inputArray " + inputArray + " - startNode " + startNode);

        StoreAtomicIndexedNodeExtension storeAtomicIndexedNodeExtension = new StoreAtomicIndexedNodeExtension(startNode);
        graph.addOrUnique(storeAtomicIndexedNodeExtension);
        final StoreAtomicIndexedNode atomicStoreNode = graph //
                .addOrUnique(new StoreAtomicIndexedNode(store.array(), store.index(), store.elementKind(), //
                        store.getBoundsCheck(), store.value(), inputArray, storeAtomicIndexedNodeExtension));
        // System.out.println("----- NEW storeatomic indexed node: " + atomicStoreNode);
        ValueNode arithmeticNode = null;
        if (value instanceof TornadoReduceAddNode) {
            TornadoReduceAddNode reduce = (TornadoReduceAddNode) value;
            if (reduce.getX() instanceof BinaryArithmeticNode) {
                arithmeticNode = reduce.getX();
            } else if (reduce.getY() instanceof BinaryArithmeticNode) {
                arithmeticNode = reduce.getY();
            } else if (reduce.getX() instanceof MarkFloatingPointIntrinsicsNode) {
                arithmeticNode = reduce.getX();
            } else if (reduce.getY() instanceof MarkFloatingPointIntrinsicsNode) {
                arithmeticNode = reduce.getY();
            }
            if (arithmeticNode == null && accumulator instanceof BinaryNode) {
                arithmeticNode = accumulator;
            }
            // System.out.println("!!!!!!!!! arithmetic node: " + arithmeticNode);
            extraOperation.put(stf, arithmeticNode);
        }
        atomicStoreNode.setNext(store.next());
        // System.out.println("------ atomic " + atomicStoreNode + " next: " +
        // atomicStoreNode.next());
        // System.out.println("------ Replace at predecessor " + predecessor);
        predecessor.replaceFirstSuccessor(store, atomicStoreNode);
        store.replaceAndDelete(atomicStoreNode);
    }

    private void performNodeReplacement(StructuredGraph graph, StoreIndexedNode store, Node predecessor, ReductionMetadataNode reductionNode) {

        ValueNode value = reductionNode.value;
        ValueNode accumulator = reductionNode.accumulator;
        ValueNode inputArray = reductionNode.inputArray;
        ValueNode startNode = reductionNode.startNode;

        StoreAtomicIndexedNodeExtension storeAtomicIndexedNodeExtension = new StoreAtomicIndexedNodeExtension(startNode);
        graph.addOrUnique(storeAtomicIndexedNodeExtension);
        final StoreAtomicIndexedNode atomicStoreNode = graph //
                .addOrUnique(new StoreAtomicIndexedNode(store.array(), store.index(), store.elementKind(), //
                        store.getBoundsCheck(), value, accumulator, inputArray, storeAtomicIndexedNodeExtension));

        ValueNode arithmeticNode = null;
        if (value instanceof TornadoReduceAddNode) {
            TornadoReduceAddNode reduce = (TornadoReduceAddNode) value;
            if (reduce.getX() instanceof BinaryArithmeticNode) {
                arithmeticNode = reduce.getX();
            } else if (reduce.getY() instanceof BinaryArithmeticNode) {
                arithmeticNode = reduce.getY();
            } else if (reduce.getX() instanceof MarkFloatingPointIntrinsicsNode) {
                arithmeticNode = reduce.getX();
            } else if (reduce.getY() instanceof MarkFloatingPointIntrinsicsNode) {
                arithmeticNode = reduce.getY();
            }
        }
        if (arithmeticNode == null && accumulator instanceof BinaryNode) {
            arithmeticNode = accumulator;
        }

        atomicStoreNode.setOptionalOperation(arithmeticNode);

        atomicStoreNode.setNext(store.next());
        predecessor.replaceFirstSuccessor(store, atomicStoreNode);
        store.replaceAndDelete(atomicStoreNode);
    }

    private boolean shouldSkip(int index, StructuredGraph graph) {
        return graph.method().isStatic() && index >= getNumberOfParameterNodes(graph);
    }

    private void processReduceAnnotation(StructuredGraph graph, int index) {

        if (shouldSkip(index, graph)) {
            return;
        }

        final ParameterNode reduceParameter = graph.getParameter(index);
        assert (reduceParameter != null);
        NodeIterable<Node> usages = reduceParameter.usages();

        for (Node node : usages) {
            if (node instanceof StoreIndexedNode) {
                StoreIndexedNode store = (StoreIndexedNode) node;
                if (store.predecessor() instanceof StoreFieldNode) {

                    ValueNode outputArray = store.array();
                    ValueNode indexToStore = store.index();
                    for (StoreFieldNode stf : graph.getNodes().filter(StoreFieldNode.class)) {
                        // System.out.println("== StoreField: " + stf);
                        if (stf.inputs().filter(BoxNode.class).isNotEmpty()) {
                            BoxNode b = stf.inputs().filter(BoxNode.class).first();
                            // System.out.println("== StoreField: " + stf + " has as input BoxNode");
                            boolean isReductionValue = checkIfReduction(b, outputArray, indexToStore);
                            // System.out.println("is reduction? " + isReductionValue);
                            if (!isReductionValue) {
                                reductionField.put(stf, false);
                                continue;
                            }

                            boolean isInALoop = checkIfVarIsInLoop(stf);
                            if (!isInALoop) {
                                reductionField.put(stf, false);
                                continue;
                            }
                            // System.out.println("stf " + stf + " is in a loop");
                            reductionField.put(stf, true);
                            ValueNode inputArray = obtainInputArray(stf, outputArray);
                            ValueNode startNode = obtainStartLoopNode(stf);
                            ReductionMetadataNode reductionNode = createReductionNode(graph, stf, inputArray, startNode);
                            // System.out.println("^*^ Reduce node: " + reductionNode);
                            Node predecessor = node.predecessor();
                            performNodeReplacementTuple(graph, store, stf, predecessor, reductionNode);
                        } else {
                            reductionField.put(stf, false);
                        }
                    }
                } else {
                    boolean isReductionValue = checkIfReduction(store);
                    if (!isReductionValue) {
                        continue;
                    }

                    boolean isInALoop = checkIfVarIsInLoop(store);
                    if (!isInALoop) {
                        continue;
                    }

                    ValueNode inputArray = obtainInputArray(store.value(), store.array());
                    ValueNode startNode = obtainStartLoopNode(store);
                    ReductionMetadataNode reductionNode = createReductionNode(graph, store, inputArray, startNode);
                    Node predecessor = node.predecessor();
                    performNodeReplacement(graph, store, predecessor, reductionNode);
                }
            } else if (node instanceof StoreFieldNode) {
                throw new TornadoRuntimeException("\n[NOT SUPPORTED] Node StoreFieldNode is not supported yet.");
            }
        }
    }

    private ValueNode obtainStartLoopNode(Node store) {
        boolean startFound = false;
        ValueNode startNode = null;
        IfNode ifNode;
        Node node = store.predecessor();
        while (!startFound) {
            if (node instanceof IfNode) {
                ifNode = (IfNode) node;
                while (!(node.predecessor() instanceof LoopBeginNode)) {
                    node = node.predecessor();
                    if (node instanceof StartNode) {
                        // node not found
                        return null;
                    }
                }
                // in this point node = loopBeginNode
                CompareNode condition = (CompareNode) ifNode.condition();
                if (condition.getX() instanceof PhiNode) {
                    PhiNode phi = (PhiNode) condition.getX();
                    startNode = phi.valueAt(0);
                    break;
                }
            }

            if (node instanceof MergeNode) {
                // When having a merge node, we follow the path any merges. We
                // choose the first
                // one, but any path will be joined.
                MergeNode merge = (MergeNode) node;
                EndNode endNode = merge.forwardEndAt(0);
                node = endNode.predecessor();
            } else if (node instanceof LoopBeginNode) {
                // It could happen that the start index is controlled by a
                // PhiNode instead of an
                // if-condition. In this case, we get the PhiNode
                LoopBeginNode loopBeginNode = (LoopBeginNode) node;
                NodeIterable<Node> usages = loopBeginNode.usages();
                for (Node u : usages) {
                    if (u instanceof PhiNode) {
                        PhiNode phiNode = (PhiNode) u;
                        startNode = phiNode.valueAt(0);
                        startFound = true;
                    }
                }
            } else {
                node = node.predecessor();
            }
        }
        return startNode;
    }

    private int getNumberOfParameterNodes(StructuredGraph graph) {
        return graph.getNodes().filter(ParameterNode.class).count();
    }

    private void findParametersWithReduceAnnotations(StructuredGraph graph) {
        final Annotation[][] parameterAnnotations = graph.method().getParameterAnnotations();
        for (int index = 0; index < parameterAnnotations.length; index++) {
            for (Annotation annotation : parameterAnnotations[index]) {
                if (annotation instanceof Reduce) {
                    // If the number of arguments does not match, then we increase the index to
                    // obtain the correct one when indexing from getParameters. This is an issue
                    // when having inheritance with interfaces from Apache Flink. See issue
                    // #185 on Github
                    if (!graph.method().isStatic() || getNumberOfParameterNodes(graph) > parameterAnnotations.length) {
                        index++;
                    }
                    processReduceAnnotation(graph, index);
                }
            }
        }
    }

    private void locateUnboxNodes(Node n, ArrayList<UnboxNode> inputs, ArrayList<Node> toBeDeleted) {
        if (n instanceof UnboxNode) {
            inputs.add((UnboxNode) n);
            return;
        } else {
            toBeDeleted.add(n);
        }
        for (Node in : n.inputs()) {
            locateUnboxNodes(in, inputs, toBeDeleted);
        }
    }
}
