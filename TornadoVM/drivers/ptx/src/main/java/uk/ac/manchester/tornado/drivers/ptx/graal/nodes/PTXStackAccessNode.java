/*
 * Copyright (c) 2020, APT Group, Department of Computer Science,
 * School of Engineering, The University of Manchester. All rights reserved.
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
package uk.ac.manchester.tornado.drivers.ptx.graal.nodes;

import jdk.vm.ci.meta.JavaConstant;
import jdk.vm.ci.meta.JavaKind;
import jdk.vm.ci.meta.PrimitiveConstant;
import org.graalvm.compiler.core.common.LIRKind;
import org.graalvm.compiler.core.common.type.StampFactory;
import org.graalvm.compiler.graph.NodeClass;
import org.graalvm.compiler.lir.ConstantValue;
import org.graalvm.compiler.lir.Variable;
import org.graalvm.compiler.nodeinfo.NodeInfo;
import org.graalvm.compiler.nodes.ConstantNode;
import org.graalvm.compiler.nodes.calc.FloatingNode;
import org.graalvm.compiler.nodes.spi.LIRLowerable;
import org.graalvm.compiler.nodes.spi.NodeLIRBuilderTool;
import uk.ac.manchester.tornado.drivers.ptx.graal.PTXArchitecture;
import uk.ac.manchester.tornado.drivers.ptx.graal.asm.PTXAssembler;
import uk.ac.manchester.tornado.drivers.ptx.graal.compiler.PTXLIRGenerator;
import uk.ac.manchester.tornado.drivers.ptx.graal.lir.PTXKind;
import uk.ac.manchester.tornado.drivers.ptx.graal.lir.PTXLIRStmt;
import uk.ac.manchester.tornado.drivers.ptx.graal.lir.PTXUnary;

import static uk.ac.manchester.tornado.drivers.ptx.graal.PTXArchitecture.globalSpace;

@NodeInfo
public class PTXStackAccessNode extends FloatingNode implements LIRLowerable {

    @Input
    private ConstantNode index;

    public static final NodeClass<PTXStackAccessNode> TYPE = NodeClass.create(PTXStackAccessNode.class);

    public PTXStackAccessNode(ConstantNode index) {
        super(TYPE, StampFactory.forKind(JavaKind.Int));
        this.index = index;
    }

    public ConstantNode getIndex() {
        return this.index;
    }

    @Override
    public void generate(NodeLIRBuilderTool gen) {
        PTXLIRGenerator tool = (PTXLIRGenerator) gen.getLIRGeneratorTool();
        LIRKind resultKind = tool.getLIRKind(stamp);
        Variable result = tool.newVariable(resultKind);

        ConstantValue indexValue = new ConstantValue(resultKind, JavaConstant.forInt(((ConstantValue) gen.operand(index)).getJavaConstant().asInt() * PTXKind.U64.getSizeInBytes()));

        tool.append(new PTXLIRStmt.LoadStmt(new PTXUnary.MemoryAccess(globalSpace, tool.getParameterAllocation(PTXArchitecture.STACK_POINTER), indexValue), result, PTXAssembler.PTXNullaryOp.LDU));
        gen.setResult(this, result);
    }
}
