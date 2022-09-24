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
 * */
package uk.ac.manchester.tornado.drivers.ptx.graal.phases;

import jdk.vm.ci.meta.DeoptimizationReason;
import org.graalvm.compiler.graph.iterators.NodeIterable;
import org.graalvm.compiler.nodes.GuardNode;
import org.graalvm.compiler.nodes.StructuredGraph;
import org.graalvm.compiler.nodes.java.AccessIndexedNode;
import org.graalvm.compiler.phases.Phase;

/**
 * After canonicalization, we might end up with a Guard of type Bounds Check
 * Exception without any array access. For those cases, we clean the graph and
 * remove also the guard (deopt) node and avoid empty basic blocks before the
 * PTX code generation.
 */
public class BoundCheckEliminationPhase extends Phase {

    @Override
    protected void run(StructuredGraph graph) {

        NodeIterable<GuardNode> guardNodes = graph.getNodes().filter(GuardNode.class);
        if (guardNodes.count() == 0) {
            return;
        }

        for (GuardNode guardNode : guardNodes) {
            DeoptimizationReason deoptReason = guardNode.getReason();
            if (deoptReason == DeoptimizationReason.BoundsCheckException) {
                if (!(guardNode.getAnchor() instanceof AccessIndexedNode)) {
                    guardNode.safeDelete();
                }
            }
        }
    }
}
