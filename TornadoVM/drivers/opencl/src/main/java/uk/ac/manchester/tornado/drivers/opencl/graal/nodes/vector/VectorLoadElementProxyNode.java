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

import static uk.ac.manchester.tornado.api.exceptions.TornadoInternalError.shouldNotReachHere;

import org.graalvm.compiler.graph.NodeClass;
import org.graalvm.compiler.nodeinfo.InputType;
import org.graalvm.compiler.nodeinfo.NodeInfo;
import org.graalvm.compiler.nodes.ConstantNode;
import org.graalvm.compiler.nodes.FixedWithNextNode;
import org.graalvm.compiler.nodes.ValueNode;

import uk.ac.manchester.tornado.drivers.opencl.graal.OCLStampFactory;
import uk.ac.manchester.tornado.drivers.opencl.graal.lir.OCLKind;

/**
 * The {@code StoreIndexedNode} represents a write to an array element.
 */
@Deprecated()
@NodeInfo(nameTemplate = "Load .s{p#lane}")
public final class VectorLoadElementProxyNode extends FixedWithNextNode {

    public static final NodeClass<VectorLoadElementProxyNode> TYPE = NodeClass.create(VectorLoadElementProxyNode.class);

    @OptionalInput(InputType.Association) ValueNode origin;
    @OptionalInput(InputType.Association) ValueNode laneOrigin;

    protected final OCLKind kind;

    protected VectorLoadElementProxyNode(NodeClass<? extends VectorLoadElementProxyNode> c, OCLKind kind, ValueNode origin, ValueNode lane) {
        super(c, OCLStampFactory.getStampFor(kind));
        this.kind = kind;
        this.origin = origin;
        this.laneOrigin = lane;
    }

    public VectorLoadElementNode tryResolve() {
        VectorLoadElementNode loadNode = null;
        if (canResolve()) {
            /*
             * If we can resolve this node properly, this operation should be
             * applied to the vector node and this node should be discarded.
             */
            VectorValueNode vector = null;
            // System.out.printf("origin: %s\n",origin);
            if (origin instanceof VectorValueNode) {
                vector = (VectorValueNode) origin;
            } // else if(origin instanceof ParameterNode){
              // vector = origin.graph().addOrUnique(new VectorValueNode(kind,
              // origin));
            else {
                shouldNotReachHere();
            }

            loadNode = new VectorLoadElementNode(kind, vector, laneOrigin);
            clearInputs();
        }

        return loadNode;
    }

    public VectorLoadElementProxyNode(VectorValueNode vector, ValueNode lane, ValueNode value) {
        this(TYPE, vector.getOCLKind(), vector, lane);
    }

    public VectorLoadElementProxyNode(OCLKind vectorKind, ValueNode origin, ValueNode lane) {
        this(TYPE, vectorKind, origin, lane);
    }

    @Override
    public boolean inferStamp() {
        return true;
        // return updateStamp(createStamp(origin, kind.getElementKind()));
    }

    public OCLKind getOCLKind() {
        return kind;
    }

    public boolean canResolve() {
        return (isOriginResolvable() && laneOrigin != null && laneOrigin instanceof ConstantNode);
    }

    private boolean isOriginResolvable() {
        return (origin != null && (origin instanceof VectorValueNode));
    }

    public ValueNode getOrigin() {
        return origin;
    }

    public void setOrigin(ValueNode value) {
        updateUsages(origin, value);
        origin = value;
    }

    public int getLane() {
        return ((ConstantNode) laneOrigin).asJavaConstant().asInt();
    }

}
