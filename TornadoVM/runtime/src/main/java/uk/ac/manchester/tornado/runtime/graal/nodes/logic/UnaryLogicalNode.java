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
package uk.ac.manchester.tornado.runtime.graal.nodes.logic;

import org.graalvm.compiler.graph.IterableNodeType;
import org.graalvm.compiler.graph.Node;
import org.graalvm.compiler.graph.NodeClass;
import org.graalvm.compiler.lir.gen.LIRGeneratorTool;
import org.graalvm.compiler.nodeinfo.InputType;
import org.graalvm.compiler.nodeinfo.NodeInfo;
import org.graalvm.compiler.nodes.LogicNode;
import org.graalvm.compiler.nodes.spi.Canonicalizable;
import org.graalvm.compiler.nodes.spi.CanonicalizerTool;
import org.graalvm.compiler.nodes.spi.NodeLIRBuilderTool;

import jdk.vm.ci.meta.Value;

@NodeInfo
public abstract class UnaryLogicalNode extends LogicNode implements IterableNodeType, Canonicalizable.Unary<LogicNode>, LogicalCompareNode {

    public static final NodeClass<UnaryLogicalNode> TYPE = NodeClass.create(UnaryLogicalNode.class);

    @Input(InputType.Condition)
    LogicNode value;

    protected UnaryLogicalNode(NodeClass<? extends UnaryLogicalNode> type, LogicNode value) {
        super(type);
        this.value = value;
    }

    @Override
    public final void generate(NodeLIRBuilderTool builder) {
        Value x = builder.operand(getValue());
        Value result = generate(builder.getLIRGeneratorTool(), x);
        builder.setResult(this, result);
    }

    abstract public Value generate(LIRGeneratorTool gen, Value x);

    @Override
    public Node canonical(CanonicalizerTool tool, LogicNode forValue) {
        return this;
    }

    @Override
    public LogicNode getValue() {
        return value;
    }

    @Override
    public LogicNode canonical(CanonicalizerTool tool) {
        return this;
    }

}
