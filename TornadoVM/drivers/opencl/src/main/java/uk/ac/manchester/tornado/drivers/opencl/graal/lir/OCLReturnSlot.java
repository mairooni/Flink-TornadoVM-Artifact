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
package uk.ac.manchester.tornado.drivers.opencl.graal.lir;

import static uk.ac.manchester.tornado.drivers.opencl.graal.asm.OCLAssemblerConstants.FRAME_REF_NAME;
import static uk.ac.manchester.tornado.drivers.opencl.mm.OCLCallStack.RETURN_VALUE_INDEX;

import org.graalvm.compiler.core.common.LIRKind;
import org.graalvm.compiler.lir.Opcode;

import jdk.vm.ci.meta.AllocatableValue;
import uk.ac.manchester.tornado.drivers.opencl.graal.asm.OCLAssembler;
import uk.ac.manchester.tornado.drivers.opencl.graal.asm.OCLAssembler.OCLNullaryOp;
import uk.ac.manchester.tornado.drivers.opencl.graal.compiler.OCLCompilationResultBuilder;

@Opcode("RETURN VALUE")
public class OCLReturnSlot extends AllocatableValue {

    private OCLNullaryOp op;

    public OCLReturnSlot(LIRKind lirKind) {
        super(lirKind);
        op = OCLNullaryOp.SLOTS_BASE_ADDRESS;
    }

    public void emit(OCLCompilationResultBuilder crb, OCLAssembler asm) {
        OCLKind type = ((OCLKind) getPlatformKind());
        asm.emit("%s[%d]", FRAME_REF_NAME, RETURN_VALUE_INDEX);
    }

    public String getStringFormat() {
        return String.format("%s[%d]", FRAME_REF_NAME, RETURN_VALUE_INDEX);
    }

    @Override
    public String toString() {
        return "RETURN_SLOT";
    }

}
