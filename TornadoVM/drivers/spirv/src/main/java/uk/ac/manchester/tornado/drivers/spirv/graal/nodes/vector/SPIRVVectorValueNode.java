/*
 * This file is part of Tornado: A heterogeneous programming framework:
 * https://github.com/beehive-lab/tornadovm
 *
 * Copyright (c) 2021, APT Group, Department of Computer Science,
 * School of Engineering, The University of Manchester. All rights reserved.
 * Copyright (c) 2009-2021, Oracle and/or its affiliates. All rights reserved.
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
package uk.ac.manchester.tornado.drivers.spirv.graal.nodes.vector;

import org.graalvm.compiler.core.common.LIRKind;
import org.graalvm.compiler.graph.NodeClass;
import org.graalvm.compiler.graph.NodeInputList;
import org.graalvm.compiler.lir.ConstantValue;
import org.graalvm.compiler.lir.Variable;
import org.graalvm.compiler.lir.gen.LIRGeneratorTool;
import org.graalvm.compiler.nodeinfo.InputType;
import org.graalvm.compiler.nodeinfo.NodeInfo;
import org.graalvm.compiler.nodes.ConstantNode;
import org.graalvm.compiler.nodes.InvokeNode;
import org.graalvm.compiler.nodes.ParameterNode;
import org.graalvm.compiler.nodes.StructuredGraph;
import org.graalvm.compiler.nodes.ValueNode;
import org.graalvm.compiler.nodes.ValuePhiNode;
import org.graalvm.compiler.nodes.calc.FloatingNode;
import org.graalvm.compiler.nodes.spi.LIRLowerable;
import org.graalvm.compiler.nodes.spi.NodeLIRBuilderTool;

import jdk.vm.ci.meta.AllocatableValue;
import jdk.vm.ci.meta.Value;
import uk.ac.manchester.tornado.drivers.spirv.graal.SPIRVStampFactory;
import uk.ac.manchester.tornado.drivers.spirv.graal.compiler.SPIRVNodeLIRBuilder;
import uk.ac.manchester.tornado.drivers.spirv.graal.lir.SPIRVKind;
import uk.ac.manchester.tornado.drivers.spirv.graal.lir.SPIRVLIROp;
import uk.ac.manchester.tornado.drivers.spirv.graal.lir.SPIRVLIRStmt;
import uk.ac.manchester.tornado.drivers.spirv.graal.lir.SPIRVVectorAssign;
import uk.ac.manchester.tornado.runtime.graal.phases.MarkVectorValueNode;

@NodeInfo(nameTemplate = "{p#spirvKind/s}")
public class SPIRVVectorValueNode extends FloatingNode implements LIRLowerable, MarkVectorValueNode {

    public static final NodeClass<SPIRVVectorValueNode> TYPE = NodeClass.create(SPIRVVectorValueNode.class);

    @OptionalInput(InputType.Association)
    private ValueNode origin;

    private SPIRVKind spirvKind;

    @Input
    NodeInputList<ValueNode> values;

    public SPIRVVectorValueNode(SPIRVKind spirvVectorKind) {
        super(TYPE, SPIRVStampFactory.getStampFor(spirvVectorKind));
        this.spirvKind = spirvVectorKind;
        this.values = new NodeInputList<>(this, spirvKind.getVectorLength());
    }

    public void initialiseToDefaultValues(StructuredGraph graph) {
        final ConstantNode defaultValue = ConstantNode.forPrimitive(spirvKind.getElementKind().getDefaultValue(), graph);
        for (int i = 0; i < spirvKind.getVectorLength(); i++) {
            setElement(i, defaultValue);
        }
    }

    public void setElement(int index, ValueNode value) {
        if (values.get(index) != null) {
            values.get(index).replaceAtUsages(value);
        } else {
            values.set(index, value);
        }
    }

    public ValueNode length() {
        return ConstantNode.forInt(spirvKind.getVectorLength());
    }

    public SPIRVKind getSPIRVKind() {
        return spirvKind;
    }

    @Override
    public void generate(NodeLIRBuilderTool gen) {
        final LIRGeneratorTool tool = gen.getLIRGeneratorTool();

        if (origin instanceof InvokeNode) {
            gen.setResult(this, gen.operand(origin));
        } else if (origin instanceof ValuePhiNode) {

            final ValuePhiNode phi = (ValuePhiNode) origin;

            final Value phiOperand = ((SPIRVNodeLIRBuilder) gen).operandForPhi(phi);
            final AllocatableValue result = (gen.hasOperand(this)) ? (Variable) gen.operand(this) : tool.newVariable(LIRKind.value(getSPIRVKind()));
            tool.append(new SPIRVLIRStmt.AssignStmt(result, phiOperand));
            gen.setResult(this, result);
        } else if (origin instanceof ParameterNode) {
            gen.setResult(this, gen.operand(origin));
        } else if (origin == null) {
            final AllocatableValue result = tool.newVariable(LIRKind.value(getSPIRVKind()));

            /*
             * Two cases:
             *
             * 1. when this vector state has elements assigned individually.
             *
             * 2. when this vector is assigned by a vector operation
             *
             */
            final int numValues = values.count();
            final ValueNode firstValue = values.first();

            if (firstValue instanceof SPIRVVectorValueNode || firstValue instanceof VectorOp) {
                tool.append(new SPIRVLIRStmt.AssignStmt(result, gen.operand(values.first())));
                gen.setResult(this, result);
            } else if (numValues > 0 && gen.hasOperand(firstValue)) {
                generateVectorAssign(gen, tool, result);
            } else {
                gen.setResult(this, result);
            }
        }
    }

    private Value getParam(NodeLIRBuilderTool gen, LIRGeneratorTool tool, int index) {
        final ValueNode valueNode = values.get(index);
        return (valueNode == null) ? new ConstantValue(LIRKind.value(spirvKind), spirvKind.getDefaultValue()) : tool.load(gen.operand(valueNode));
    }

    // THis construct generates the equivalent of the following OpenCL Code:
    // vtype = (a, b, c, d);
    private void generateVectorAssign(NodeLIRBuilderTool gen, LIRGeneratorTool tool, AllocatableValue result) {
        SPIRVLIROp assignExpr;
        Value s0;
        Value s1;
        Value s2;
        Value s3;
        Value s4;
        Value s5;
        Value s6;
        Value s7;
        LIRKind lirKind;

        // check if first parameter is a vector
        s0 = getParam(gen, tool, 0);
        if (spirvKind.getVectorLength() >= 2) {
            if (((SPIRVKind) s0.getPlatformKind()).isVector()) {
                gen.setResult(this, s0);
                return;
            }
        }

        switch (spirvKind.getVectorLength()) {
            case 2:
                s1 = getParam(gen, tool, 1);
                gen.getLIRGeneratorTool().getLIRKind(stamp);
                lirKind = gen.getLIRGeneratorTool().getLIRKind(stamp);
                assignExpr = new SPIRVVectorAssign.AssignVectorExpr(lirKind, s0, s1);
                break;
            case 3:
                s1 = getParam(gen, tool, 1);
                s2 = getParam(gen, tool, 2);
                lirKind = gen.getLIRGeneratorTool().getLIRKind(stamp);
                assignExpr = new SPIRVVectorAssign.AssignVectorExpr(lirKind, s0, s1, s2);
                break;
            case 4:
                s1 = getParam(gen, tool, 1);
                s2 = getParam(gen, tool, 2);
                s3 = getParam(gen, tool, 3);
                lirKind = gen.getLIRGeneratorTool().getLIRKind(stamp);
                assignExpr = new SPIRVVectorAssign.AssignVectorExpr(lirKind, s0, s1, s2, s3);
                break;
            case 8:
                s1 = getParam(gen, tool, 1);
                s2 = getParam(gen, tool, 2);
                s3 = getParam(gen, tool, 3);
                s4 = getParam(gen, tool, 4);
                s5 = getParam(gen, tool, 5);
                s6 = getParam(gen, tool, 6);
                s7 = getParam(gen, tool, 7);
                lirKind = gen.getLIRGeneratorTool().getLIRKind(stamp);
                assignExpr = new SPIRVVectorAssign.AssignVectorExpr(lirKind, s0, s1, s2, s3, s4, s5, s6, s7);
                break;
            default:
                throw new RuntimeException("Operation type not supported");
        }
        tool.append(new SPIRVLIRStmt.AssignStmt(result, assignExpr));
        gen.setResult(this, result);
    }
}
