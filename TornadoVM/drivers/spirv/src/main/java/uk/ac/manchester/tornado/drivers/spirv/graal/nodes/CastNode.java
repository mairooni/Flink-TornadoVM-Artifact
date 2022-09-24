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
package uk.ac.manchester.tornado.drivers.spirv.graal.nodes;

import org.graalvm.compiler.core.common.LIRKind;
import org.graalvm.compiler.core.common.calc.FloatConvert;
import org.graalvm.compiler.core.common.type.Stamp;
import org.graalvm.compiler.graph.NodeClass;
import org.graalvm.compiler.lir.Variable;
import org.graalvm.compiler.nodeinfo.NodeInfo;
import org.graalvm.compiler.nodes.ValueNode;
import org.graalvm.compiler.nodes.calc.FloatingNode;
import org.graalvm.compiler.nodes.spi.LIRLowerable;
import org.graalvm.compiler.nodes.spi.NodeLIRBuilderTool;

import jdk.vm.ci.meta.Value;
import uk.ac.manchester.tornado.drivers.spirv.graal.compiler.SPIRVLIRGenerator;
import uk.ac.manchester.tornado.drivers.spirv.graal.lir.SPIRVKind;
import uk.ac.manchester.tornado.drivers.spirv.graal.lir.SPIRVLIRStmt;
import uk.ac.manchester.tornado.drivers.spirv.graal.lir.SPIRVUnary;
import uk.ac.manchester.tornado.runtime.graal.phases.MarkCastNode;

@NodeInfo(shortName = "SPIRVCastNode")
public class CastNode extends FloatingNode implements LIRLowerable, MarkCastNode {

    public static final NodeClass<CastNode> TYPE = NodeClass.create(CastNode.class);

    @Input
    protected ValueNode value;
    protected FloatConvert op;

    public CastNode(Stamp stamp, FloatConvert op, ValueNode value) {
        super(TYPE, stamp);
        this.value = value;
        this.op = op;
    }

    private SPIRVUnary.CastOperations resolveOp(LIRKind lirKind, Value value) {
        switch (op) {
            case I2F:
                return new SPIRVUnary.CastIToFloat(lirKind, value, SPIRVKind.OP_TYPE_FLOAT_32);
            case I2D:
                return new SPIRVUnary.CastIToFloat(lirKind, value, SPIRVKind.OP_TYPE_FLOAT_64);
            case D2F:
                return new SPIRVUnary.CastFloatDouble(lirKind, value, SPIRVKind.OP_TYPE_FLOAT_32);
            case F2D:
                return new SPIRVUnary.CastFloatDouble(lirKind, value, SPIRVKind.OP_TYPE_FLOAT_64);
            case L2D:
                return new SPIRVUnary.CastFloatDouble(lirKind, value, SPIRVKind.OP_TYPE_FLOAT_64);
            case L2F:
                return new SPIRVUnary.CastFloatToLong(lirKind, value, SPIRVKind.OP_TYPE_FLOAT_32);
            case F2I:
                return new SPIRVUnary.CastFloatToInt(lirKind, value, SPIRVKind.OP_TYPE_INT_32);
            case D2L:
            case F2L:
            case D2I:
            default:
                throw new RuntimeException("Conversion Cast Operation unimplemented: " + op);
        }
    }

    @Override
    public void generate(NodeLIRBuilderTool generator) {
        SPIRVLIRGenerator gen = (SPIRVLIRGenerator) generator.getLIRGeneratorTool();
        LIRKind lirKind = gen.getLIRKind(stamp);
        final Variable result = gen.newVariable(lirKind);
        Value value = generator.operand(this.value);
        SPIRVUnary.CastOperations cast = resolveOp(lirKind, value);
        gen.append(new SPIRVLIRStmt.AssignStmt(result, cast));
        generator.setResult(this, result);
    }
}
