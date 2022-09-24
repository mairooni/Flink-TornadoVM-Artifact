package uk.ac.manchester.tornado.drivers.opencl.graal.phases;

import jdk.vm.ci.meta.Constant;
import jdk.vm.ci.meta.RawConstant;
import org.graalvm.compiler.core.common.type.StampFactory;
import org.graalvm.compiler.graph.Node;
import org.graalvm.compiler.graph.iterators.NodeIterable;
import org.graalvm.compiler.nodes.*;
import org.graalvm.compiler.nodes.calc.AddNode;
import org.graalvm.compiler.nodes.calc.MulNode;
import org.graalvm.compiler.nodes.calc.SignExtendNode;
import org.graalvm.compiler.nodes.memory.ReadNode;
import org.graalvm.compiler.nodes.memory.WriteNode;
import org.graalvm.compiler.phases.Phase;
import uk.ac.manchester.tornado.api.flink.FlinkCompilerInfo;
import uk.ac.manchester.tornado.drivers.opencl.graal.lir.OCLAddressNode;
import uk.ac.manchester.tornado.drivers.opencl.graal.nodes.GlobalThreadIdNode;
import uk.ac.manchester.tornado.runtime.FlinkCompilerInfoIntermediate;

import java.util.HashMap;

public class TornadoMatrixOffset extends Phase {

    private boolean isMatrix;
    private int rowSize;
    private int columnSize1;
    private int matrixTypeSize;
    private HashMap readOuterIndex;
    private HashMap writeOuterIndex;

    private void flinkSetCompInfo(FlinkCompilerInfo fcomp) {
        this.isMatrix = fcomp.getIsMatrix();
        this.rowSize = fcomp.getRowSizeMatrix1();
        this.columnSize1 = fcomp.getColumnSizeMatrix1();
        this.matrixTypeSize = fcomp.getMatrixTypeSize();
        this.readOuterIndex = fcomp.getReadOuterIndex();
        this.writeOuterIndex = fcomp.getWriteOuterIndex();
    }

    @Override
    protected void run(StructuredGraph graph) {
        FlinkCompilerInfo fcomp = FlinkCompilerInfoIntermediate.getFlinkCompilerInfo();
        if (fcomp != null) {
            flinkSetCompInfo(fcomp);
        }

        if (isMatrix) {
            Constant element = new RawConstant(matrixTypeSize);
            ConstantNode elementSize = new ConstantNode(element, StampFactory.positiveInt());
            graph.addWithoutUnique(elementSize);

            Constant columnBytes = new RawConstant(columnSize1 * matrixTypeSize);
            ConstantNode columnByteSize = new ConstantNode(columnBytes, StampFactory.positiveInt());
            graph.addWithoutUnique(columnByteSize);

            Constant rowBytes = new RawConstant(rowSize * matrixTypeSize);
            ConstantNode rowByteSize = new ConstantNode(rowBytes, StampFactory.positiveInt());
            graph.addWithoutUnique(rowByteSize);
            // matrix element: rowByteSize * i + elementSize * j (i=outer index, j = inner
            // index)

            // locate global thread id phi node
            NodeIterable<ValuePhiNode> phiNodes = graph.getNodes().filter(ValuePhiNode.class);
            ValuePhiNode globalPh = null;
            for (ValuePhiNode ph : phiNodes) {
                if (ph.inputs().filter(GlobalThreadIdNode.class).count() > 0) {
                    globalPh = ph;
                    break;
                }
            }
            // ReadNodes
            NodeIterable<ReadNode> readNodes = graph.getNodes().filter(ReadNode.class);

            if (readNodes.isNotEmpty()) {
                for (ReadNode r : readNodes) {
                    OCLAddressNode ocl = (OCLAddressNode) r.inputs().first();
                    ParameterNode param = ocl.inputs().filter(ParameterNode.class).first();
                    if (param == null) {
                        System.out.println("OCLAddress " + ocl + " has no param input");
                        return;
                    }
                    // if (param.toString().contains("1")) {
                    // for node that has input param (1)
                    // outer index: global ph
                    // inner index: index of ocl
                    ValuePhiNode outer = (ValuePhiNode) readOuterIndex.get(param);
                    // rowBytes * outerIndex
                    MulNode m1;
                    if (param.toString().contains("1")) {
                        m1 = new MulNode(columnByteSize, outer);
                    } else {
                        m1 = new MulNode(rowByteSize, outer);
                    }
                    graph.addWithoutUnique(m1);
                    // elementSize * innerIndex
                    SignExtendNode[] arr = new SignExtendNode[1];
                    getSignExtend(ocl, arr);
                    SignExtendNode s = arr[0];
                    MulNode m2 = new MulNode(elementSize, s);
                    graph.addWithoutUnique(m2);
                    // rowBytes * outerIndex + elementSize * innerIndex
                    AddNode add = new AddNode(m1, m2);
                    graph.addWithoutUnique(add);
                    // replace
                    AddNode adOCL = ocl.inputs().filter(AddNode.class).first();
                    if (adOCL.usages().count() > 1) {
                        // adOCL.copyWithInputs(true);
                        Node adIN = null;
                        ConstantNode constN = null;
                        for (Node in : adOCL.inputs()) {
                            if (!(in instanceof ConstantNode)) {
                                adIN = in;
                                // break;
                            } else {
                                constN = (ConstantNode) in;
                            }
                        }
                        // System.out.println("Replace " + adIN + " with " + add);
                        // adOCL.replaceFirstInput(adIN, add);
                        AddNode newAdd = new AddNode(add, constN);
                        graph.addWithoutUnique(newAdd);
                        ocl.replaceFirstInput(adOCL, newAdd);
                        if (adIN.usages().count() == 0) {
                            adIN.safeDelete();
                        }
                    } else {
                        Node adIN = null;
                        for (Node in : adOCL.inputs()) {
                            if (!(in instanceof ConstantNode)) {
                                adIN = in;
                                break;
                            }
                        }
                        adOCL.replaceFirstInput(adIN, add);
                        if (adIN.usages().count() == 0) {
                            adIN.safeDelete();
                        }
                    }
                    // } else if (param.toString().contains("2")) {
                    // System.out.println("OCL " + ocl + " has 2st param array input, read node " +
                    // r);
                    // ValuePhiNode outer = (ValuePhiNode) readOuterIndex.get(param);
                    // System.out.println("Outer index: " + outer);

                    // }
                }
            }

            // WriteNodes
            NodeIterable<WriteNode> writeNodes = graph.getNodes().filter(WriteNode.class);
            // Constant rowBytesW = new RawConstant(64);
            // ConstantNode rowByteSizeW = new ConstantNode(rowBytesW,
            // StampFactory.positiveInt());
            // graph.addWithoutUnique(rowByteSizeW);
            if (writeNodes.isNotEmpty()) {
                for (WriteNode wr : writeNodes) {
                    OCLAddressNode ocl = (OCLAddressNode) wr.inputs().first();
                    ParameterNode param = ocl.inputs().filter(ParameterNode.class).first();
                    if (param == null) {
                        System.out.println("OCLAddress " + ocl + " has no param input");
                        return;
                    }
                    // if (param.toString().contains("1")) {
                    // for node that has input param (1)
                    // outer index: global ph
                    // inner index: index of ocl
                    ValuePhiNode outer = (ValuePhiNode) writeOuterIndex.get(param);
                    // rowBytes * outerIndex
                    MulNode m1 = new MulNode(rowByteSize, outer);
                    graph.addWithoutUnique(m1);
                    // elementSize * innerIndex
                    SignExtendNode[] arr = new SignExtendNode[1];
                    getSignExtend(ocl, arr);
                    SignExtendNode s = arr[0];
                    MulNode m2 = new MulNode(elementSize, s);
                    graph.addWithoutUnique(m2);
                    // rowBytes * outerIndex + elementSize * innerIndex
                    AddNode add = new AddNode(m1, m2);
                    graph.addWithoutUnique(add);
                    // replace
                    AddNode adOCL = ocl.inputs().filter(AddNode.class).first();
                    Node adIN = null;
                    for (Node in : adOCL.inputs()) {
                        if (!(in instanceof ConstantNode)) {
                            adIN = in;
                            break;
                        }
                    }
                    adOCL.replaceFirstInput(adIN, add);
                    adIN.safeDelete();
                }
            }
        }
    }

    void getSignExtend(Node n, SignExtendNode[] arr) {
        if (n instanceof SignExtendNode) {
            arr[0] = (SignExtendNode) n;
            return;
        }

        for (Node in : n.inputs()) {
            getSignExtend(in, arr);
        }
    }
}
