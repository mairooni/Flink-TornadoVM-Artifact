/*
 * This file is part of Tornado: A heterogeneous programming framework: 
 * https://github.com/beehive-lab/tornadovm
 *
 * Copyright (c) 2013-2020, APT Group, Department of Computer Science,
 * The University of Manchester. All rights reserved.
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

import static uk.ac.manchester.tornado.drivers.opencl.graal.asm.OCLAssemblerConstants.ADDRESS_OF;
import static uk.ac.manchester.tornado.drivers.opencl.graal.asm.OCLAssemblerConstants.SQUARE_BRACKETS_CLOSE;
import static uk.ac.manchester.tornado.drivers.opencl.graal.asm.OCLAssemblerConstants.SQUARE_BRACKETS_OPEN;

import jdk.vm.ci.meta.AllocatableValue;
import org.graalvm.compiler.core.common.LIRKind;
import org.graalvm.compiler.lir.LIRInstruction.Use;
import org.graalvm.compiler.lir.Opcode;

import jdk.vm.ci.meta.AllocatableValue;
import jdk.vm.ci.meta.Value;
import org.graalvm.compiler.lir.Variable;
import uk.ac.manchester.tornado.drivers.opencl.graal.OCLArchitecture;
import uk.ac.manchester.tornado.drivers.opencl.graal.OCLArchitecture.OCLMemoryBase;
import uk.ac.manchester.tornado.drivers.opencl.graal.asm.OCLAssembler;
import uk.ac.manchester.tornado.drivers.opencl.graal.asm.OCLAssembler.OCLUnaryOp;
import uk.ac.manchester.tornado.drivers.opencl.graal.asm.OCLAssembler.OCLUnaryTemplate;
import uk.ac.manchester.tornado.drivers.opencl.graal.asm.OCLAssemblerConstants;
import uk.ac.manchester.tornado.drivers.opencl.graal.compiler.OCLCompilationResultBuilder;
import uk.ac.manchester.tornado.drivers.opencl.graal.meta.OCLMemorySpace;
import uk.ac.manchester.tornado.drivers.opencl.graal.nodes.OCLBarrierNode.OCLMemFenceFlags;

public class OCLUnary {

    /**
     * Abstract operation which consumes one inputs
     */
    protected static class UnaryConsumer extends OCLLIROp {

        @Opcode
        protected final OCLUnaryOp opcode;

        @Use
        protected Value value;

        UnaryConsumer(OCLUnaryOp opcode, LIRKind lirKind, Value value) {
            super(lirKind);
            this.opcode = opcode;
            this.value = value;
        }

        public Value getValue() {
            return value;
        }

        public OCLUnaryOp getOpcode() {
            return opcode;
        }

        @Override
        public void emit(OCLCompilationResultBuilder crb, OCLAssembler asm) {
            opcode.emit(crb, value);
        }

        @Override
        public String toString() {
            return String.format("%s %s", opcode.toString(), value);
        }

    }

    public static class Expr extends UnaryConsumer {

        public Expr(OCLUnaryOp opcode, LIRKind lirKind, Value value) {
            super(opcode, lirKind, value);
        }

    }

    public static class Intrinsic extends UnaryConsumer {

        public Intrinsic(OCLUnaryOp opcode, LIRKind lirKind, Value value) {
            super(opcode, lirKind, value);
        }

        @Override
        public String toString() {
            return String.format("%s(%s)", opcode.toString(), value);
        }

    }

    public static class IntrinsicAtomicFetch extends UnaryConsumer {

        public IntrinsicAtomicFetch(OCLUnaryOp opcode, LIRKind lirKind, Value value) {
            super(opcode, lirKind, value);
        }

        @Override
        public void emit(OCLCompilationResultBuilder crb, OCLAssembler asm) {
            asm.emit(toString());
        }

        @Override
        public String toString() {
            return String.format("%s(&%s, 1, memory_order_relaxed)", opcode.toString(), value.toString());
        }
    }

    public static class IntrinsicAtomicInc extends UnaryConsumer {

        private int index;
        private static final String arrayName = OCLArchitecture.atomicSpace.getName();

        public IntrinsicAtomicInc(OCLUnaryOp opcode, LIRKind lirKind, Value value, int index) {
            super(opcode, lirKind, value);
            this.index = index;
        }

        @Override
        public void emit(OCLCompilationResultBuilder crb, OCLAssembler asm) {
            asm.emit(toString());
        }

        @Override
        public String toString() {
            return String.format("%s(&%s[%s])", opcode.toString(), arrayName, index);
        }
    }

    public static class IntrinsicAtomicDeclaration extends UnaryConsumer {

        AllocatableValue lhs;

        /*
         * The opcode is the initializer intrinsic to use
         */
        public IntrinsicAtomicDeclaration(OCLUnaryOp opcode, AllocatableValue lhs, Value initialValue) {
            super(opcode, LIRKind.Illegal, initialValue);
            this.lhs = lhs;
        }

        @Override
        public void emit(OCLCompilationResultBuilder crb, OCLAssembler asm) {
            StringBuffer lineGlobalScope = new StringBuffer();
            lineGlobalScope.append("__global atomic_int ");
            lineGlobalScope.append(asm.getStringValue(crb, lhs));
            lineGlobalScope.append(OCLAssemblerConstants.ASSIGN);
            lineGlobalScope.append(opcode.toString());
            lineGlobalScope.append(OCLAssemblerConstants.OPEN_PARENTHESIS);
            lineGlobalScope.append(asm.getStringValue(crb, value));
            lineGlobalScope.append(OCLAssemblerConstants.CLOSE_PARENTHESIS);
            lineGlobalScope.append(OCLAssemblerConstants.STMT_DELIMITER);
            lineGlobalScope.append("\n");
            asm.emitLineGlobal(lineGlobalScope.toString());
        }
    }

    public static class LoadOCLStack extends UnaryConsumer {

        public LoadOCLStack(OCLUnaryOp opcode, LIRKind lirKind, Value value) {
            super(opcode, lirKind, value);
        }

        @Override
        public void emit(OCLCompilationResultBuilder crb, OCLAssembler asm) {
            asm.emit(opcode.toString());
            asm.emit("[");
            asm.emitValueOrOp(crb, value);
            asm.emit("]");
        }

        @Override
        public String toString() {
            return String.format("%s[%s] ", opcode.toString(), value);
        }

    }

    public static class Barrier extends UnaryConsumer {

        OCLMemFenceFlags flags;

        public Barrier(OCLUnaryOp opcode, OCLMemFenceFlags flags) {
            super(opcode, LIRKind.Illegal, null);
            this.flags = flags;
        }

        @Override
        public void emit(OCLCompilationResultBuilder crb, OCLAssembler asm) {
            asm.emit(toString());
        }

        @Override
        public String toString() {
            return String.format("%s(CLK_%s_MEM_FENCE)", opcode.toString(), flags.toString().toUpperCase());
        }

    }

    public static class FloatCast extends UnaryConsumer {

        public FloatCast(OCLUnaryOp opcode, LIRKind lirKind, Value value) {
            super(opcode, lirKind, value);
        }

        @Override
        public void emit(OCLCompilationResultBuilder crb, OCLAssembler asm) {
            asm.emit("isnan(");
            asm.emitValueOrOp(crb, value);
            asm.emit(")? 0 : ");
            opcode.emit(crb, value);
        }

        @Override
        public String toString() {
            return String.format("isnan(%s) ? 0 : %s %s", value, opcode.toString(), value);
        }
    }

    public static class MemoryAccess extends UnaryConsumer {

        private final OCLMemoryBase base;
        private final boolean needsBase;
        private Value index;
        private AllocatableValue assignedTo;

        MemoryAccess(OCLMemoryBase base, Value value, boolean needsBase) {
            super(null, LIRKind.Illegal, value);
            this.base = base;
            this.needsBase = needsBase;
        }

        MemoryAccess(OCLMemoryBase base, Value value, Value index, boolean needsBase) {
            super(null, LIRKind.Illegal, value);
            this.base = base;
            this.index = index;
            this.needsBase = needsBase;
        }

        private boolean shouldEmitRelativeAddress(OCLCompilationResultBuilder crb) {
            return needsBase || (!keepIntegerIndexing() && crb.getDeviceContext().useRelativeAddresses());
        }

        private boolean keepIntegerIndexing() {
            return (base.memorySpace == OCLMemorySpace.PRIVATE || base.memorySpace == OCLMemorySpace.LOCAL);
        }

        @Override
        public void emit(OCLCompilationResultBuilder crb, OCLAssembler asm) {
            if (shouldEmitRelativeAddress(crb)) {
                asm.emitSymbol(ADDRESS_OF);
                asm.emit(base.name);
                asm.emitSymbol(SQUARE_BRACKETS_OPEN);
                asm.emitValue(crb, value);
                asm.emitSymbol(SQUARE_BRACKETS_CLOSE);
            } else {
                asm.emitValue(crb, value);
            }
        }

        public OCLMemoryBase getBase() {
            return base;
        }

        public Value getIndex() {
            return index;
        }

        public void assignTo(AllocatableValue loadedTo) {
            assignedTo = loadedTo;
        }

        public AllocatableValue assignedTo() {
            return assignedTo;
        }

        @Override
        public String toString() {
            return String.format("%s", value);
        }
    }

    public static class OCLAddressCast extends UnaryConsumer {

        private final OCLMemoryBase base;

        public OCLAddressCast(OCLMemoryBase base, LIRKind lirKind) {
            super(OCLUnaryTemplate.CAST_TO_POINTER, lirKind, null);
            this.base = base;
        }

        @Override
        public void emit(OCLCompilationResultBuilder crb, OCLAssembler asm) {
            OCLKind oclKind = getOCLPlatformKind();
            asm.emit(((OCLUnaryTemplate) opcode).getTemplate(), base.memorySpace.name() + " " + oclKind.toString());
        }

        OCLMemorySpace getMemorySpace() {
            return base.memorySpace;
        }

    }

}
