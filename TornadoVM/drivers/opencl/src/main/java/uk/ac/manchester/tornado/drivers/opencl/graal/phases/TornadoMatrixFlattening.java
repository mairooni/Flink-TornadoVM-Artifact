package uk.ac.manchester.tornado.drivers.opencl.graal.phases;

import jdk.vm.ci.meta.Constant;
import jdk.vm.ci.meta.JavaKind;
import jdk.vm.ci.meta.RawConstant;
import org.graalvm.compiler.core.common.type.StampFactory;
import org.graalvm.compiler.graph.Node;
import org.graalvm.compiler.graph.iterators.NodeIterable;
import org.graalvm.compiler.nodes.*;
import org.graalvm.compiler.nodes.calc.FloatingNode;
import org.graalvm.compiler.nodes.extended.BoxNode;
import org.graalvm.compiler.nodes.extended.UnboxNode;
import org.graalvm.compiler.nodes.java.ArrayLengthNode;
import org.graalvm.compiler.nodes.java.LoadIndexedNode;
import org.graalvm.compiler.nodes.java.StoreIndexedNode;
import org.graalvm.compiler.phases.BasePhase;
import uk.ac.manchester.tornado.api.flink.FlinkCompilerInfo;
import uk.ac.manchester.tornado.runtime.FlinkCompilerInfoIntermediate;
import uk.ac.manchester.tornado.runtime.graal.nodes.NewArrayNonVirtualizableNode;
import uk.ac.manchester.tornado.runtime.graal.phases.TornadoHighTierContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

/**
 * This compiler phase was created for the Flink-TornadoVM integration. The
 * purpose of this phase is to read and write the matrix array as data in a
 * flattened array.
 */
public class TornadoMatrixFlattening extends BasePhase<TornadoHighTierContext> {

    private boolean isMatrix;
    private int rowSize1;
    private int rowSize2;
    private int columnSize2;
    private Class matrixType;

    private void flinkSetCompInfo(FlinkCompilerInfo fcomp) {
        this.isMatrix = fcomp.getIsMatrix();
        this.rowSize1 = fcomp.getRowSizeMatrix1();
        this.rowSize2 = fcomp.getRowSizeMatrix2();
        this.columnSize2 = fcomp.getColumnSizeMatrix2();
        this.matrixType = fcomp.getMatrixType();
    }

    @Override
    protected void run(StructuredGraph graph, TornadoHighTierContext context) {
        FlinkCompilerInfo fcomp = FlinkCompilerInfoIntermediate.getFlinkCompilerInfo();
        if (fcomp != null) {
            flinkSetCompInfo(fcomp);
        }
        if (isMatrix) {

            // Step 1: Replace arrayLength nodes with a simple constant
            Constant row1Size = new RawConstant(rowSize1);
            ConstantNode loop1Limit = new ConstantNode(row1Size, StampFactory.positiveInt());
            graph.addWithoutUnique(loop1Limit);
            Constant column2Size = new RawConstant(columnSize2);
            ConstantNode loop2Limit = new ConstantNode(column2Size, StampFactory.positiveInt());
            graph.addWithoutUnique(loop2Limit);
            NodeIterable<ArrayLengthNode> arrLengthNodes = graph.getNodes().filter(ArrayLengthNode.class);
            Queue<Node> limits = new LinkedList<>();
            limits.add(loop1Limit);
            limits.add(loop2Limit);
            if (!arrLengthNodes.isEmpty()) {
                for (ArrayLengthNode arl : arrLengthNodes) {
                    arl.replaceAtUsages(limits.remove());
                    if (arl.predecessor() instanceof LoadIndexedNode) {
                        LoadIndexedNode ld = (LoadIndexedNode) arl.predecessor();
                        if ((ld.usages().contains(arl) && ld.usages().count() == 1) || ld.usages().count() == 0) {
                            // the only purpose of this loadindexednode is to define the arraylength
                            // therefore we can delete it
                            removeFixed(ld);
                        }
                    }
                    removeFixed(arl);
                }
            }

            ArrayList<Node> nodesToBeDeleted = new ArrayList<>();
            // Step 2: Handle LoadIndexedNodes
            HashMap readOuterIndex = new HashMap();
            NodeIterable<LoadIndexedNode> loadIndexedNodes = graph.getNodes().filter(LoadIndexedNode.class);
            ArrayList<LoadIndexedNode> newLoadIndexedNodes = new ArrayList<>();
            if (!loadIndexedNodes.isEmpty()) {
                for (LoadIndexedNode ldidx : loadIndexedNodes) {
                    if (ldidx.inputs().filter(ParameterNode.class).count() == 1 && !newLoadIndexedNodes.contains(ldidx)) {
                        ValuePhiNode outerIndex = ldidx.inputs().filter(ValuePhiNode.class).first();
                        ParameterNode param = ldidx.inputs().filter(ParameterNode.class).first();
                        readOuterIndex.put(param, outerIndex);
                        // Input of LoadIndexedNode is Parameter node
                        LoadIndexedNode ldIndexedUsage = ldidx.usages().filter(LoadIndexedNode.class).first();
                        if (ldIndexedUsage == null && ldidx.usages().filter(PiNode.class).count() > 0) {
                            PiNode pi = ldidx.usages().filter(PiNode.class).first();
                            ldIndexedUsage = pi.usages().filter(LoadIndexedNode.class).first();
                        }
                        ValuePhiNode phi = ldIndexedUsage.inputs().filter(ValuePhiNode.class).first();
                        LoadIndexedNode newLdIdx = new LoadIndexedNode(null, ldidx.array(), phi, null, JavaKind.fromJavaClass(matrixType));
                        graph.addWithoutUnique(newLdIdx);
                        replaceFixed(ldIndexedUsage, newLdIdx);
                        nodesToBeDeleted.add(ldidx);
                        // get unbox node
                        Node suc = newLdIdx.successors().first();
                        while (!(suc instanceof UnboxNode)) {
                            nodesToBeDeleted.add(suc);
                            suc = suc.successors().first();
                        }
                        for (Node us : suc.usages()) {
                            us.replaceFirstInput(suc, newLdIdx);
                        }
                        // newLdIdx.replaceAtUsages(suc);
                        // System.out.println("Node " + newLdIdx + " will replace " + suc + " at
                        // usages");
                        nodesToBeDeleted.add(suc);
                        newLoadIndexedNodes.add(newLdIdx);
                    }
                }

                for (Node n : nodesToBeDeleted) {
                    if (n instanceof FloatingNode) {
                        n.safeDelete();
                    } else {
                        removeFixed(n);
                    }
                }
                fcomp.setReadOuterIndex(readOuterIndex);
            }

            // deal with storeindexednodes
            NodeIterable<StoreIndexedNode> storeIndexedNodes = graph.getNodes().filter(StoreIndexedNode.class);
            ArrayList<LoadIndexedNode> newStoreIndexedNodes = new ArrayList<>();
            StoreIndexedNode storeParam = null;
            int i = storeIndexedNodes.count();
            if (storeIndexedNodes.isNotEmpty()) {
                HashMap writeOuterIndex = new HashMap();
                ParameterNode param = null;
                // find array that will store results
                for (StoreIndexedNode st : storeIndexedNodes) {
                    if (st.inputs().filter(ParameterNode.class).count() > 0) {
                        param = st.inputs().filter(ParameterNode.class).first();
                        ValuePhiNode outerIndex = st.inputs().filter(ValuePhiNode.class).first();
                        writeOuterIndex.put(param, outerIndex);
                        storeParam = st; // .inputs().filter(ParameterNode.class).first();
                        break;
                    }
                }

                for (StoreIndexedNode st : storeIndexedNodes) {
                    if (i == 0)
                        break;

                    if (st.inputs().filter(BoxNode.class).count() > 0) {
                        BoxNode box = st.inputs().filter(BoxNode.class).first();
                        // ValuePhiNode outerIndex = box.inputs().filter(ValuePhiNode.class).first();
                        // writeOuterIndex.put(param, outerIndex);
                        ValueNode inputData = (ValueNode) box.inputs().first();
                        ValuePhiNode index = st.inputs().filter(ValuePhiNode.class).first();
                        StoreIndexedNode newStore = new StoreIndexedNode(storeParam.array(), index, null, null, JavaKind.fromJavaClass(matrixType), (ValueNode) inputData);
                        graph.addWithoutUnique(newStore);
                        replaceFixed(st, newStore);
                        removeFixed(box);
                    }
                    i--;
                }

                removeFixed(storeParam);
                fcomp.setWriteOuterIndex(writeOuterIndex);
            }

            // remove newArrayNonVirtualizable node
            NodeIterable<NewArrayNonVirtualizableNode> nonVirt = graph.getNodes().filter(NewArrayNonVirtualizableNode.class);

            for (NewArrayNonVirtualizableNode nV : nonVirt) {
                removeFromFrameState(nV, graph);
                removeFixed(nV);
            }

        }
    }

    public static void removeFixed(Node n) {
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

    public static void replaceFixed(Node n, Node other) {
        Node pred = n.predecessor();
        Node suc = n.successors().first();
        n.replaceFirstSuccessor(suc, null);
        n.replaceAtPredecessor(other);
        pred.replaceFirstSuccessor(n, other);
        other.replaceFirstSuccessor(null, suc);
        // removeFromFrameState(n, graph);
        for (Node us : n.usages()) {
            n.removeUsage(us);
        }
        n.clearInputs();
        n.safeDelete();

        return;
    }

    public static void removeFromFrameState(Node del, StructuredGraph graph) {
        for (Node n : graph.getNodes()) {
            if (n instanceof FrameState) {
                FrameState f = (FrameState) n;
                if (f.values().contains(del)) {
                    f.values().remove(del);
                }
                // } else if (f.inputs().contains(del)) {
                // f.replaceFirstInput(del, null);
                // }
            }
        }
    }
}
