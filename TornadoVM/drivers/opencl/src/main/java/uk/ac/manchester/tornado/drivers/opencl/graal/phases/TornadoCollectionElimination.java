package uk.ac.manchester.tornado.drivers.opencl.graal.phases;

import jdk.vm.ci.meta.Constant;
import jdk.vm.ci.meta.JavaKind;
import jdk.vm.ci.meta.RawConstant;
import org.graalvm.compiler.core.common.type.StampFactory;
import org.graalvm.compiler.graph.Node;
import org.graalvm.compiler.nodes.*;
import org.graalvm.compiler.nodes.calc.IntegerLessThanNode;
import org.graalvm.compiler.nodes.extended.UnboxNode;
import org.graalvm.compiler.nodes.java.LoadFieldNode;
import org.graalvm.compiler.nodes.java.LoadIndexedNode;
import org.graalvm.compiler.nodes.java.MethodCallTargetNode;
import org.graalvm.compiler.nodes.java.StoreFieldNode;
import org.graalvm.compiler.phases.BasePhase;
import uk.ac.manchester.tornado.api.flink.FlinkCompilerInfo;
import uk.ac.manchester.tornado.drivers.opencl.graal.lir.OCLNullary;
import uk.ac.manchester.tornado.runtime.FlinkCompilerInfoIntermediate;
import uk.ac.manchester.tornado.runtime.graal.phases.TornadoHighTierContext;

import java.util.ArrayList;

public class TornadoCollectionElimination extends BasePhase<TornadoHighTierContext> {

    private int sizeOfCollection;
    private boolean broadcastedDataset;
    private String collectionName;
    private boolean isMatrix;
    private String storeFieldName = "parameter";
    private int rowSize2;

    void flinkSetCompInfo(FlinkCompilerInfo flinkCompilerInfo) {
        this.broadcastedDataset = flinkCompilerInfo.getBroadcastedDataset();
        this.collectionName = flinkCompilerInfo.getCollectionName();
        this.sizeOfCollection = flinkCompilerInfo.getBroadCollectionSize();
        this.isMatrix = flinkCompilerInfo.getIsMatrix();
        this.rowSize2 = flinkCompilerInfo.getRowSizeMatrix2();
    }

    @Override
    protected void run(StructuredGraph graph, TornadoHighTierContext context) {
        FlinkCompilerInfo fcomp = FlinkCompilerInfoIntermediate.getFlinkCompilerInfo();
        if (fcomp != null) {
            flinkSetCompInfo(fcomp);
        }

        if (broadcastedDataset) {

            boolean forEach = false;

            for (Node n : graph.getNodes()) {
                if (n instanceof InvokeNode) {
                    InvokeNode in = (InvokeNode) n;
                    if (in.toString().contains("hasNext")) {
                        forEach = true;
                    }
                }
            }

            if (forEach) {

                System.out.println("[ERROR] We currently do not support for each iterators. Please replace with with a regular for loop.");
                return;

            } else {

                boolean iterateCollection = false;

                for (Node n : graph.getNodes()) {
                    if (n instanceof InvokeNode) {
                        if (n.toString().contains("Collection")) {
                            iterateCollection = true;
                            break;
                        }
                    }
                }

                if (iterateCollection && !isMatrix) {
                    for (Node n : graph.getNodes()) {
                        if (n instanceof LoadFieldNode && n.toString().contains(collectionName)) {

                            ArrayList<Node> nodesToBeRemoved = new ArrayList<>();
                            nodesToBeRemoved.add(n);

                            Node pred = n.predecessor();
                            Node currSuc = n.successors().first();

                            Node newSuc = currSuc;

                            while (newSuc instanceof InvokeNode || newSuc instanceof FixedGuardNode) {
                                nodesToBeRemoved.add(newSuc);
                                newSuc = newSuc.successors().first();
                            }

                            for (Node nodes : graph.getNodes()) {
                                if (nodes instanceof IntegerLessThanNode) {
                                    for (Node in : nodes.inputs()) {
                                        if (in instanceof InvokeNode) {
                                            Constant loopLimitConst = new RawConstant(sizeOfCollection);
                                            ConstantNode loopLimit = new ConstantNode(loopLimitConst, StampFactory.positiveInt());
                                            graph.addWithoutUnique(loopLimit);
                                            nodes.replaceFirstInput(in, loopLimit);
                                            // test dft
                                            in.replaceAtUsages(loopLimit);
                                        }
                                    }
                                }
                            }

                            Node newSucPrev = newSuc.predecessor();

                            // break link
                            newSucPrev.replaceFirstSuccessor(newSuc, null);

                            n.replaceAtPredecessor(newSuc);

                            pred.replaceFirstSuccessor(n, newSuc);

                            for (int i = 0; i < nodesToBeRemoved.size(); i++) {
                                Node del = nodesToBeRemoved.get(i);
                                if (del instanceof LoadFieldNode) {
                                    del.safeDelete();
                                } else {
                                    for (Node in : del.inputs()) {
                                        in.safeDelete();
                                    }
                                    del.safeDelete();
                                }
                            }

                        }
                    }
                } else {
                    // --- HACK: In order to be able to access the data we have a dummy
                    // LoadIndex node right after the start node of the graph
                    // The location of this node is fixed because we have an array access
                    // on the Flink map skeleton right before the computation
                    // We need to extract this array in order to use it as input for the
                    // nodes that use the broadcasted dataset

                    LoadIndexedNode secondInput = null;
                    boolean extraFixedNodes = false;
                    // find parameter p(2)
                    for (Node n : graph.getNodes()) {
                        if (n instanceof StartNode) {
                            if (n.successors().first() instanceof LoadIndexedNode) {
                                secondInput = (LoadIndexedNode) n.successors().first();
                                extraFixedNodes = true;
                                break;
                            }
                        }
                    }
                    ParameterNode param2 = null;
                    if (secondInput == null) {
                        for (ParameterNode param : graph.getNodes().filter(ParameterNode.class)) {
                            if (param.index() == 2) {
                                param2 = param;
                                break;
                            }
                        }
                    }

                    if (secondInput == null && param2 == null)
                        return;
                    // find the usages of the broadcasted dataset

                    // PiNode piNode = null;
                    FixedGuardNode fx = null;
                    LoadFieldNode lfBroadcasted = null;
                    StoreFieldNode storeBroadcasted = null;
                    ConstantNode indxOffset = null;
                    ArrayList<Node> collectionFixedNodes = new ArrayList<>();
                    ValuePhiNode broadIndex = null;
                    for (Node n : graph.getNodes()) {
                        if (n instanceof InvokeNode && n.toString().contains("get")) {
                            for (Node in : n.inputs()) {
                                if (in instanceof MethodCallTargetNode) {
                                    for (Node mtIn : in.inputs()) {
                                        if (mtIn instanceof ConstantNode) {
                                            indxOffset = (ConstantNode) mtIn.copyWithInputs();
                                        } else if (mtIn instanceof ValuePhiNode) {
                                            broadIndex = (ValuePhiNode) mtIn;
                                        }
                                    }
                                }
                            }

                            if (n.successors().first() instanceof FixedGuardNode) {
                                FixedGuardNode fxG = (FixedGuardNode) n.successors().first();
                                for (Node us : fxG.usages()) {
                                    if (us instanceof PiNode) {
                                        // piNode = (PiNode) us;
                                        fx = fxG;
                                        break;
                                    }
                                }
                            }
                        } else if (n instanceof LoadFieldNode && n.toString().contains(collectionName)) {
                            lfBroadcasted = (LoadFieldNode) n;
                        } else if (n instanceof StoreFieldNode && n.toString().contains(storeFieldName)) {
                            storeBroadcasted = (StoreFieldNode) n;
                        }
                    }

                    ArrayList<Class> tupleFieldKindSecondDataSet = new ArrayList<>();
                    if (fcomp != null) {
                        tupleFieldKindSecondDataSet = fcomp.getTupleFieldKindSecondDataSet();
                    } else {
                        System.out.println("[ERROR]: COMPILER INFO NOT INITIALIZED");
                        return;
                    }
                    // Constant position = new RawConstant(0);
                    // ConstantNode indxOffset = new ConstantNode(position,
                    // StampFactory.positiveInt());
                    // graph.addWithoutUnique(indxOffset);
                    LoadIndexedNode ldInxdBroadcasted;
                    if (broadIndex == null) {
                        if (secondInput != null) {
                            ldInxdBroadcasted = new LoadIndexedNode(null, secondInput.array(), indxOffset, null, JavaKind.fromJavaClass(tupleFieldKindSecondDataSet.get(0)));
                        } else {
                            ldInxdBroadcasted = new LoadIndexedNode(null, param2, indxOffset, null, JavaKind.fromJavaClass(tupleFieldKindSecondDataSet.get(0)));
                        }
                    } else {
                        if (isMatrix) {
                            if (secondInput != null) {
                                ldInxdBroadcasted = new LoadIndexedNode(null, secondInput.array(), broadIndex, null, JavaKind.fromJavaClass(fcomp.getMatrixType()));
                            } else {
                                ldInxdBroadcasted = new LoadIndexedNode(null, param2, broadIndex, null, JavaKind.fromJavaClass(fcomp.getMatrixType()));
                            }
                        } else {
                            if (secondInput != null) {
                                ldInxdBroadcasted = new LoadIndexedNode(null, secondInput.array(), broadIndex, null, JavaKind.fromJavaClass(tupleFieldKindSecondDataSet.get(0)));
                            } else {
                                ldInxdBroadcasted = new LoadIndexedNode(null, param2, broadIndex, null, JavaKind.fromJavaClass(tupleFieldKindSecondDataSet.get(0)));
                            }
                        }
                    }
                    graph.addWithoutUnique(ldInxdBroadcasted);

                    // ldInxdBroadcasted.replaceAtUsages(fx);
                    PiNode p = (PiNode) fx.usages().first();
                    p.replaceFirstInput(fx, ldInxdBroadcasted);
                    // if (piNode != null) {
                    // for (Node piUs : piNode.usages()) {
                    // System.out.println("Usage: " + piUs);
                    // for (Node piUsInput : piUs.inputs()) {
                    // if (piUsInput == piNode) {
                    // System.out.println("- Replace input pi with loadIndex");
                    // piUsInput.replaceFirstInput(piNode, ldInxdBroadcasted);
                    // }
                    // }
                    // }
                    // }
                    // ldInxdBroadcasted.replaceAtUsages(piNode);
                    //
                    // for (Node ldinxUs : ldInxdBroadcasted.usages()) {
                    // System.out.println("+ LoadIndexed Usage: " + ldinxUs);
                    // }

                    // remove fixed nodes related to the broadcasted collection
                    // identify the fixed nodes related to the collection
                    if (storeBroadcasted != null) {
                        Node n = lfBroadcasted;
                        while (true) {
                            collectionFixedNodes.add(n);
                            if (n instanceof StoreFieldNode && n.toString().contains(storeFieldName)) {
                                break;
                            }
                            n = n.successors().first();
                        }

                        Node suc = storeBroadcasted.successors().first();
                        Node pred = lfBroadcasted.predecessor();
                        storeBroadcasted.replaceFirstSuccessor(suc, null);
                        lfBroadcasted.replaceAtPredecessor(ldInxdBroadcasted);
                        pred.replaceFirstSuccessor(lfBroadcasted, ldInxdBroadcasted);
                        ldInxdBroadcasted.replaceFirstSuccessor(null, suc);

                        for (Node fixedColl : collectionFixedNodes) {
                            // removeFromFrameState(fixedColl, graph);
                            fixedColl.safeDelete();
                        }
                    } else {
                        if (isMatrix) {
                            Node n = lfBroadcasted;
                            Node last = null;
                            while (true) {
                                collectionFixedNodes.add(n);
                                if (n instanceof FixedGuardNode && (n.successors().first() instanceof LoadIndexedNode)) {
                                    last = n; // .successors().first();
                                    break;
                                }
                                n = n.successors().first();
                            }

                            Node suc = last.successors().first();
                            Node pred = lfBroadcasted.predecessor();
                            last.replaceFirstSuccessor(suc, null);
                            lfBroadcasted.replaceAtPredecessor(ldInxdBroadcasted);
                            pred.replaceFirstSuccessor(lfBroadcasted, ldInxdBroadcasted);
                            ldInxdBroadcasted.replaceFirstSuccessor(null, suc);
                            for (Node fixedColl : collectionFixedNodes) {
                                // removeFromFrameState(fixedColl, graph);
                                fixedColl.safeDelete();
                            }
                        } else {
                            Node n = lfBroadcasted;
                            Node last = null;
                            while (true) {
                                collectionFixedNodes.add(n);
                                if (n instanceof FixedGuardNode && (n.successors().first() instanceof UnboxNode)) {
                                    collectionFixedNodes.add(n.successors().first());
                                    last = n.successors().first(); // .successors().first();
                                    break;
                                }
                                n = n.successors().first();
                            }
                            Node suc = last.successors().first();
                            Node pred = lfBroadcasted.predecessor();
                            last.replaceFirstSuccessor(suc, null);
                            lfBroadcasted.replaceAtPredecessor(ldInxdBroadcasted);
                            pred.replaceFirstSuccessor(lfBroadcasted, ldInxdBroadcasted);
                            ldInxdBroadcasted.replaceFirstSuccessor(null, suc);
                            last.replaceAtUsages(ldInxdBroadcasted);
                            for (Node fixedColl : collectionFixedNodes) {
                                // removeFromFrameState(fixedColl, graph);
                                fixedColl.safeDelete();
                            }
                        }
                    }

                    if (iterateCollection && isMatrix) {
                        for (InvokeNode inv : graph.getNodes().filter(InvokeNode.class)) {
                            if (inv.toString().contains("List.get")) {
                                // replace with a loadindexnode
                                ArrayList<Node> nodesToBeDeleted = new ArrayList<>();
                                if (inv.successors().first() instanceof FixedGuardNode) {
                                    FixedGuardNode fxg = (FixedGuardNode) inv.successors().first();
                                    nodesToBeDeleted.add(fxg);
                                    Node pred = inv.predecessor();
                                    // find the predecessor and the successor for deletion
                                    Node sucDel = fxg.successors().first();
                                    Node predDel;
                                    while (true) {
                                        if (pred instanceof LoadFieldNode) {
                                            nodesToBeDeleted.add(pred);
                                            predDel = pred.predecessor();
                                            break;
                                        } else {
                                            nodesToBeDeleted.add(pred);
                                        }
                                        pred = pred.predecessor();
                                    }
                                    ConstantNode arrayIDX = null;
                                    for (Node in : inv.inputs()) {
                                        if (in instanceof MethodCallTargetNode) {
                                            for (Node mcIn : in.inputs()) {
                                                if (mcIn instanceof ConstantNode) {
                                                    arrayIDX = (ConstantNode) mcIn;
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    // LoadIndexedNode loadget = new LoadIndexedNode(null, secondInput.array(),
                                    // arrayIDX, null, JavaKind.fromJavaClass(fcomp.getMatrixType()));
                                    // graph.addWithoutUnique(loadget);
                                    nodesToBeDeleted.add(inv);
                                    for (Node n : nodesToBeDeleted) {
                                        removeFixed(n, graph);
                                    }
                                    // graph.addAfterFixed((FixedWithNextNode) predDel, loadget);
                                    // graph.addBeforeFixed((FixedNode) sucDel, loadget);
                                    // loadget.replaceAtPredecessor(predDel.predecessor());
                                }
                            } else if (inv.toString().contains("Collection.size")) {
                                // replace with a constant node
                                if (inv.predecessor() instanceof LoadFieldNode) {
                                    Node pred = inv.predecessor();
                                    removeFixed(pred, graph);
                                }
                                Constant loopLimitConst = new RawConstant(rowSize2);
                                ConstantNode loopLimit = new ConstantNode(loopLimitConst, StampFactory.positiveInt());
                                graph.addWithoutUnique(loopLimit);

                                for (Node us : inv.usages()) {
                                    if (us instanceof IntegerLessThanNode) {
                                        us.replaceFirstInput(inv, loopLimit);
                                        break;
                                    }
                                }
                                removeFixed(inv, graph);
                            }
                        }
                    }

                    if (extraFixedNodes) {
                        // remove dummy loadIndex node
                        Node predD = secondInput.predecessor();
                        Node lf = secondInput.successors().first();
                        Node fixG = lf.successors().first();
                        Node sucD = fixG.successors().first();

                        fixG.replaceFirstSuccessor(sucD, null);
                        secondInput.replaceAtPredecessor(sucD);
                        predD.replaceFirstSuccessor(secondInput, sucD);

                        lf.safeDelete();
                        secondInput.safeDelete();
                        fixG.safeDelete();
                    }
                }
            }

        }
    }

    public static void removeFixed(Node n, StructuredGraph graph) {
        Node pred = n.predecessor();
        Node suc = n.successors().first();

        n.replaceFirstSuccessor(suc, null);
        n.replaceAtPredecessor(suc);
        pred.replaceFirstSuccessor(n, suc);

        // removeFromFrameState(n, graph);

        for (Node us : n.usages()) {
            n.removeUsage(us);
        }
        n.clearInputs();

        n.safeDelete();

        return;
    }
    // public static void removeFromFrameState(Node del, StructuredGraph graph) {
    // for (Node n : graph.getNodes()) {
    // if (n instanceof FrameState) {
    // FrameState f = (FrameState) n;
    // if (f.values().contains(del)) {
    // f.values().remove(del);
    // }
    // // } else if (f.inputs().contains(del)) {
    // // f.replaceFirstInput(del, null);
    // // }
    // }
    // }
    // }
}
