/*
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
 * Authors: James Clarkson
 *
 */
package uk.ac.manchester.tornado.drivers.opencl.graal.nodes.vector;

import org.graalvm.compiler.core.common.LIRKind;
import org.graalvm.compiler.core.common.type.Stamp;
import org.graalvm.compiler.graph.Node;
import org.graalvm.compiler.graph.NodeClass;
import org.graalvm.compiler.lir.Variable;
import org.graalvm.compiler.nodeinfo.NodeInfo;
import org.graalvm.compiler.nodes.NodeView;
import org.graalvm.compiler.nodes.ValueNode;
import org.graalvm.compiler.nodes.calc.BinaryNode;
import org.graalvm.compiler.nodes.spi.CanonicalizerTool;
import org.graalvm.compiler.nodes.spi.LIRLowerable;
import org.graalvm.compiler.nodes.spi.NodeLIRBuilderTool;

import jdk.vm.ci.meta.Value;
import uk.ac.manchester.tornado.drivers.common.logging.Logger;
import uk.ac.manchester.tornado.drivers.opencl.graal.OCLStamp;
import uk.ac.manchester.tornado.drivers.opencl.graal.OCLStampFactory;
import uk.ac.manchester.tornado.drivers.opencl.graal.asm.OCLAssembler.OCLBinaryOp;
import uk.ac.manchester.tornado.drivers.opencl.graal.lir.OCLBinary;
import uk.ac.manchester.tornado.drivers.opencl.graal.lir.OCLKind;
import uk.ac.manchester.tornado.drivers.opencl.graal.lir.OCLLIRStmt.AssignStmt;

@NodeInfo(shortName = "+")
public class VectorAddNode extends BinaryNode implements LIRLowerable, VectorOp {

    public static final NodeClass<VectorAddNode> TYPE = NodeClass.create(VectorAddNode.class);

    public VectorAddNode(OCLKind kind, ValueNode x, ValueNode y) {
        super(TYPE, OCLStampFactory.getStampFor(kind), x, y);
    }

    @Override
    public Stamp foldStamp(Stamp stampX, Stamp stampY) {
        Stamp currentStamp = stamp(NodeView.DEFAULT);
        if (currentStamp instanceof OCLStamp) {
            return currentStamp;
        }
        return (stampX instanceof OCLStamp) ? stampX.join(stampY) : stampY.join(stampX);
    }

    @Override
    public void generate(NodeLIRBuilderTool gen) {
        LIRKind lirKind = gen.getLIRGeneratorTool().getLIRKind(stamp);
        final Variable result = gen.getLIRGeneratorTool().newVariable(lirKind);

        final Value input1 = gen.operand(x);
        final Value input2 = gen.operand(y);

        Logger.traceBuildLIR(Logger.BACKEND.OpenCL, "emitVectorAdd: %s + %s", input1, input2);
        gen.getLIRGeneratorTool().append(new AssignStmt(result, new OCLBinary.Expr(OCLBinaryOp.ADD, gen.getLIRGeneratorTool().getLIRKind(stamp), input1, input2)));
        gen.setResult(this, result);
    }

    @Override
    public Node canonical(CanonicalizerTool ct, ValueNode t, ValueNode t1) {
        return this;
    }

    @Override
    public ValueNode canonical(CanonicalizerTool ct) {
        return this;
    }

}
