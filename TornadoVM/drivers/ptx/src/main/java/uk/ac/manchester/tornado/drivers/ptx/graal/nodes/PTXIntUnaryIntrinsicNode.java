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

import static uk.ac.manchester.tornado.api.exceptions.TornadoInternalError.shouldNotReachHere;

import org.graalvm.compiler.core.common.LIRKind;
import org.graalvm.compiler.core.common.type.StampFactory;
import org.graalvm.compiler.graph.Node;
import org.graalvm.compiler.graph.NodeClass;
import org.graalvm.compiler.lir.Variable;
import org.graalvm.compiler.lir.gen.ArithmeticLIRGeneratorTool;
import org.graalvm.compiler.nodeinfo.NodeInfo;
import org.graalvm.compiler.nodes.ConstantNode;
import org.graalvm.compiler.nodes.ValueNode;
import org.graalvm.compiler.nodes.calc.UnaryNode;
import org.graalvm.compiler.nodes.spi.ArithmeticLIRLowerable;
import org.graalvm.compiler.nodes.spi.CanonicalizerTool;
import org.graalvm.compiler.nodes.spi.NodeLIRBuilderTool;

import jdk.vm.ci.meta.JavaKind;
import jdk.vm.ci.meta.Value;
import jdk.vm.ci.meta.ValueKind;
import uk.ac.manchester.tornado.api.exceptions.TornadoInternalError;
import uk.ac.manchester.tornado.drivers.common.logging.Logger;
import uk.ac.manchester.tornado.drivers.ptx.graal.lir.PTXArithmeticTool;
import uk.ac.manchester.tornado.drivers.ptx.graal.lir.PTXBuiltinTool;
import uk.ac.manchester.tornado.drivers.ptx.graal.lir.PTXKind;
import uk.ac.manchester.tornado.drivers.ptx.graal.lir.PTXLIRStmt.AssignStmt;
import uk.ac.manchester.tornado.runtime.graal.phases.MarkIntIntrinsicNode;

@NodeInfo(nameTemplate = "{p#operation/s}")
public class PTXIntUnaryIntrinsicNode extends UnaryNode implements ArithmeticLIRLowerable, MarkIntIntrinsicNode {

    protected PTXIntUnaryIntrinsicNode(ValueNode x, Operation op, JavaKind kind) {
        super(TYPE, StampFactory.forKind(kind), x);
        this.operation = op;
    }

    public static final NodeClass<PTXIntUnaryIntrinsicNode> TYPE = NodeClass.create(PTXIntUnaryIntrinsicNode.class);
    protected final Operation operation;

    @Override
    public String getOperation() {
        return operation.toString();
    }

    public enum Operation {
        ABS, POPCOUNT
    }

    public Operation operation() {
        return operation;
    }

    public static ValueNode create(ValueNode x, Operation op, JavaKind kind) {
        ValueNode c = tryConstantFold(x, op, kind);
        if (c != null) {
            return c;
        }
        return new PTXIntUnaryIntrinsicNode(x, op, kind);
    }

    protected static ValueNode tryConstantFold(ValueNode x, Operation op, JavaKind kind) {
        ConstantNode result = null;

        if (x.isConstant()) {
            if (kind == JavaKind.Int) {
                int ret = doCompute(x.asJavaConstant().asInt(), op);
                result = ConstantNode.forInt(ret);
            } else if (kind == JavaKind.Long) {
                long ret = doCompute(x.asJavaConstant().asLong(), op);
                result = ConstantNode.forLong(ret);
            }
        }
        return result;
    }

    @Override
    public void generate(NodeLIRBuilderTool builder, ArithmeticLIRGeneratorTool lirGen) {
        Logger.traceBuildLIR(Logger.BACKEND.PTX, "emitPTXIntUnaryIntrinsic: op=%s, x=%s", operation, getValue());
        PTXBuiltinTool gen = ((PTXArithmeticTool) lirGen).getGen().getPtxBuiltinTool();
        Value x = builder.operand(getValue());
        Value result;
        ValueKind valueKind = null;
        switch (operation()) {
            case ABS:
                result = gen.genIntAbs(x);
                valueKind = result.getValueKind();
                break;
            case POPCOUNT:
                result = gen.genIntPopcount(x);
                valueKind = LIRKind.value(PTXKind.U32);
                break;
            default:
                throw shouldNotReachHere();
        }
        Variable var = builder.getLIRGeneratorTool().newVariable(valueKind);
        builder.getLIRGeneratorTool().append(new AssignStmt(var, result));
        builder.setResult(this, var);

    }

    private static long doCompute(long value, Operation op) {
        switch (op) {
            case ABS:
                return Math.abs(value);
            case POPCOUNT:
                return Long.bitCount(value);
            default:
                throw new TornadoInternalError("unknown op %s", op);
        }
    }

    private static int doCompute(int value, Operation op) {
        switch (op) {
            case ABS:
                return Math.abs(value);
            case POPCOUNT:
                return Integer.bitCount(value);
            default:
                throw new TornadoInternalError("unknown op %s", op);
        }
    }

    @Override
    public Node canonical(CanonicalizerTool tool, ValueNode value) {
        ValueNode c = tryConstantFold(value, operation(), getStackKind());
        if (c != null) {
            return c;
        }
        return this;
    }

}
