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

package uk.ac.manchester.tornado.drivers.ptx.graal.compiler;

import jdk.vm.ci.code.CallingConvention;
import org.graalvm.compiler.core.common.CompilationIdentifier;
import org.graalvm.compiler.core.common.alloc.RegisterAllocationConfig;
import org.graalvm.compiler.lir.LIR;
import org.graalvm.compiler.lir.Variable;
import org.graalvm.compiler.lir.framemap.FrameMapBuilder;
import org.graalvm.compiler.lir.gen.LIRGenerationResult;
import uk.ac.manchester.tornado.drivers.ptx.graal.lir.PTXKind;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static uk.ac.manchester.tornado.api.exceptions.TornadoInternalError.guarantee;

public class PTXLIRGenerationResult extends LIRGenerationResult {

    public static class VariableData {
        public boolean isArray;
        public Variable variable;

        public VariableData(Variable variable, boolean isArray) {
            this.variable = variable;
            this.isArray = isArray;
        }
    }

    private final Map<PTXKind, Set<VariableData>> variableTable;
    private final Map<PTXKind, Variable> returnVariables;

    public PTXLIRGenerationResult(CompilationIdentifier identifier, LIR lir, FrameMapBuilder frameMapBuilder, RegisterAllocationConfig registerAllocationConfig, CallingConvention callingConvention) {
        super(identifier, lir, frameMapBuilder, registerAllocationConfig, callingConvention);

        variableTable = new HashMap<>();
        returnVariables = new HashMap<>();
    }

    public int insertVariableAndGetIndex(Variable var, boolean isArray) {
        guarantee(var.getPlatformKind() instanceof PTXKind, "invalid variable kind: %s", var.getValueKind());
        PTXKind kind = (PTXKind) var.getPlatformKind();

        variableTable.computeIfAbsent(kind, k -> new HashSet<>()).add(new VariableData(var, isArray));
        int arrayCount = isArray ? 0 : (int) variableTable.get(kind).stream().filter(varData -> varData.isArray).count();
        return variableTable.get(kind).size() - arrayCount - 1;
    }

    public Map<PTXKind, Set<VariableData>> getVariableTable() {
        return variableTable;
    }

    public void setReturnVariable(Variable var) {
        PTXKind ptxKind = (PTXKind) var.getPlatformKind();

        if (!returnVariables.containsKey(ptxKind)) {
            returnVariables.put(ptxKind, var);
        }
    }

    public Variable getReturnVariable(PTXKind kind) {
        return returnVariables.get(kind);
    }
}
