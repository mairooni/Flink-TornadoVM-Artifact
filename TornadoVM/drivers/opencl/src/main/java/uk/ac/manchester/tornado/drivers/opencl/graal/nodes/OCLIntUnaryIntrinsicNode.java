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
package uk.ac.manchester.tornado.drivers.opencl.graal.nodes;

import static uk.ac.manchester.tornado.api.exceptions.TornadoInternalError.shouldNotReachHere;

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
import uk.ac.manchester.tornado.api.exceptions.TornadoInternalError;
import uk.ac.manchester.tornado.drivers.opencl.graal.lir.OCLArithmeticTool;
import uk.ac.manchester.tornado.drivers.opencl.graal.lir.OCLBuiltinTool;
import uk.ac.manchester.tornado.drivers.opencl.graal.lir.OCLLIRStmt.AssignStmt;
import uk.ac.manchester.tornado.runtime.graal.phases.MarkIntIntrinsicNode;

@NodeInfo(nameTemplate = "{p#operation/s}")
public class OCLIntUnaryIntrinsicNode extends UnaryNode implements ArithmeticLIRLowerable, MarkIntIntrinsicNode {

    protected OCLIntUnaryIntrinsicNode(ValueNode x, Operation op, JavaKind kind) {
        super(TYPE, StampFactory.forKind(kind), x);
        this.operation = op;
    }

    public static final NodeClass<OCLIntUnaryIntrinsicNode> TYPE = NodeClass.create(OCLIntUnaryIntrinsicNode.class);
    protected final Operation operation;

    @Override
    public String getOperation() {
        return operation.toString();
    }

    public enum Operation {
        ABS, CLZ, POPCOUNT
    }

    public Operation operation() {
        return operation;
    }

    public static ValueNode create(ValueNode x, Operation op, JavaKind kind) {
        ValueNode c = tryConstantFold(x, op, kind);
        if (c != null) {
            return c;
        }
        return new OCLIntUnaryIntrinsicNode(x, op, kind);
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
        OCLBuiltinTool gen = ((OCLArithmeticTool) lirGen).getGen().getOCLBuiltinTool();
        Value x = builder.operand(getValue());
        Value result;
        switch (operation()) {
            case ABS:
                result = gen.genIntAbs(x);
                break;
            case CLZ:
                result = gen.genIntClz(x);
                break;
            case POPCOUNT:
                result = gen.genIntPopcount(x);
                break;
            default:
                throw shouldNotReachHere();
        }
        Variable var = builder.getLIRGeneratorTool().newVariable(result.getValueKind());
        builder.getLIRGeneratorTool().append(new AssignStmt(var, result));
        builder.setResult(this, var);

    }

    private static long doCompute(long value, Operation op) {
        switch (op) {
            case ABS:
                return Math.abs(value);
            case CLZ:
                return Long.numberOfLeadingZeros(value);
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
            case CLZ:
                return Integer.numberOfLeadingZeros(value);
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
