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
package uk.ac.manchester.tornado.drivers.spirv.graal.compiler.lir;

import static uk.ac.manchester.tornado.api.exceptions.TornadoInternalError.guarantee;

import org.graalvm.compiler.core.common.LIRKind;
import org.graalvm.compiler.core.common.calc.FloatConvert;
import org.graalvm.compiler.lir.LIRFrameState;
import org.graalvm.compiler.lir.Variable;
import org.graalvm.compiler.lir.gen.ArithmeticLIRGenerator;

import jdk.vm.ci.meta.AllocatableValue;
import jdk.vm.ci.meta.PlatformKind;
import jdk.vm.ci.meta.Value;
import jdk.vm.ci.meta.ValueKind;
import uk.ac.manchester.tornado.drivers.common.logging.Logger;
import uk.ac.manchester.tornado.drivers.spirv.graal.SPIRVArchitecture;
import uk.ac.manchester.tornado.drivers.spirv.graal.SPIRVLIRKindTool;
import uk.ac.manchester.tornado.drivers.spirv.graal.asm.SPIRVAssembler.SPIRVBinaryOp;
import uk.ac.manchester.tornado.drivers.spirv.graal.compiler.SPIRVLIRGenerator;
import uk.ac.manchester.tornado.drivers.spirv.graal.lir.SPIRVBinary;
import uk.ac.manchester.tornado.drivers.spirv.graal.lir.SPIRVKind;
import uk.ac.manchester.tornado.drivers.spirv.graal.lir.SPIRVLIROp;
import uk.ac.manchester.tornado.drivers.spirv.graal.lir.SPIRVLIRStmt;
import uk.ac.manchester.tornado.drivers.spirv.graal.lir.SPIRVTernary;
import uk.ac.manchester.tornado.drivers.spirv.graal.lir.SPIRVUnary;
import uk.ac.manchester.tornado.drivers.spirv.graal.lir.SPIRVUnary.MemoryAccess;
import uk.ac.manchester.tornado.drivers.spirv.graal.lir.SPIRVUnary.SPIRVAddressCast;
import uk.ac.manchester.tornado.drivers.spirv.graal.lir.SPIRVVectorElementSelect;

public class SPIRVArithmeticTool extends ArithmeticLIRGenerator {

    public SPIRVLIRGenerator getGen() {
        return (SPIRVLIRGenerator) getLIRGen();
    }

    public SPIRVLIROp genBinaryExpr(SPIRVBinaryOp op, LIRKind lirKind, Value x, Value y) {
        return new SPIRVBinary.Expr(op, lirKind, x, y);
    }

    public Variable emitBinaryAssign(SPIRVBinaryOp op, LIRKind lirKind, Value x, Value y) {
        final Variable result = getGen().newVariable(lirKind);
        getGen().append(new SPIRVLIRStmt.AssignStmt(result, genBinaryExpr(op, lirKind, x, y)));
        return result;
    }

    public Variable emitBinaryVectorAssign(SPIRVBinaryOp op, LIRKind lirKind, Value x, Value y) {
        SPIRVKind spirvKind = (SPIRVKind) lirKind.getPlatformKind();
        final Variable result = getGen().newVariable(LIRKind.value(spirvKind.getElementKind()));
        getGen().append(new SPIRVLIRStmt.AssignStmt(result, new SPIRVBinary.VectorOperation(op, lirKind, x, y)));
        return result;
    }

    @Override
    protected boolean isNumericInteger(PlatformKind platformKind) {
        if (!(platformKind instanceof SPIRVKind)) {
            throw new RuntimeException("Invalid Platform Kind");
        }
        return ((SPIRVKind) platformKind).isInteger();
    }

    @Override
    protected Variable emitAdd(LIRKind resultKind, Value a, Value b, boolean setFlags) {
        Logger.traceBuildLIR(Logger.BACKEND.SPIRV, "[µInstructions] emitAdd: %s + %s -- RESULT KIND: %s", a, b, resultKind);
        SPIRVKind kind = (SPIRVKind) resultKind.getPlatformKind();
        SPIRVBinaryOp binaryOp;
        switch (kind) {
            case OP_TYPE_VECTOR3_INT_8:
            case OP_TYPE_VECTOR4_INT_8:
            case OP_TYPE_VECTOR2_INT_32:
            case OP_TYPE_VECTOR3_INT_32:
            case OP_TYPE_VECTOR4_INT_32:
            case OP_TYPE_VECTOR8_INT_32:
                return emitBinaryVectorAssign(SPIRVBinaryOp.ADD_INTEGER, resultKind, a, b);
            case OP_TYPE_VECTOR2_FLOAT_32:
            case OP_TYPE_VECTOR3_FLOAT_32:
            case OP_TYPE_VECTOR4_FLOAT_32:
            case OP_TYPE_VECTOR8_FLOAT_32:
            case OP_TYPE_VECTOR2_FLOAT_64:
            case OP_TYPE_VECTOR3_FLOAT_64:
            case OP_TYPE_VECTOR4_FLOAT_64:
            case OP_TYPE_VECTOR8_FLOAT_64:
                return emitBinaryVectorAssign(SPIRVBinaryOp.ADD_FLOAT, resultKind, a, b);
            case OP_TYPE_INT_8:
            case OP_TYPE_INT_16:
            case OP_TYPE_INT_64:
            case OP_TYPE_INT_32:
                binaryOp = SPIRVBinaryOp.ADD_INTEGER;
                break;
            case OP_TYPE_FLOAT_32:
            case OP_TYPE_FLOAT_64:
                binaryOp = SPIRVBinaryOp.ADD_FLOAT;
                break;
            default:
                throw new RuntimeException("Type not supported: " + resultKind);
        }
        return emitBinaryAssign(binaryOp, resultKind, a, b);
    }

    @Override
    protected Variable emitSub(LIRKind resultKind, Value a, Value b, boolean setFlags) {
        Logger.traceBuildLIR(Logger.BACKEND.SPIRV, "[µInstructions] emitSub: %s - %s", a, b);
        SPIRVKind kind = (SPIRVKind) resultKind.getPlatformKind();
        SPIRVBinaryOp binaryOp;
        switch (kind) {
            case OP_TYPE_VECTOR3_INT_8:
            case OP_TYPE_VECTOR4_INT_8:
            case OP_TYPE_VECTOR2_INT_32:
            case OP_TYPE_VECTOR3_INT_32:
            case OP_TYPE_VECTOR4_INT_32:
            case OP_TYPE_VECTOR8_INT_32:
                return emitBinaryVectorAssign(SPIRVBinaryOp.SUB_INTEGER, resultKind, a, b);
            case OP_TYPE_VECTOR2_FLOAT_32:
            case OP_TYPE_VECTOR3_FLOAT_32:
            case OP_TYPE_VECTOR4_FLOAT_32:
            case OP_TYPE_VECTOR8_FLOAT_32:
            case OP_TYPE_VECTOR2_FLOAT_64:
            case OP_TYPE_VECTOR3_FLOAT_64:
            case OP_TYPE_VECTOR4_FLOAT_64:
            case OP_TYPE_VECTOR8_FLOAT_64:
                return emitBinaryVectorAssign(SPIRVBinaryOp.SUB_FLOAT, resultKind, a, b);
            case OP_TYPE_INT_8:
            case OP_TYPE_INT_64:
            case OP_TYPE_INT_32:
                binaryOp = SPIRVBinaryOp.SUB_INTEGER;
                break;
            case OP_TYPE_FLOAT_64:
            case OP_TYPE_FLOAT_32:
                binaryOp = SPIRVBinaryOp.SUB_FLOAT;
                break;
            default:
                throw new RuntimeException("Type not supported: " + resultKind);
        }
        return emitBinaryAssign(binaryOp, resultKind, a, b);
    }

    @Override
    public Value emitNegate(Value input) {
        Logger.traceBuildLIR(Logger.BACKEND.SPIRV, "emitNegate:  - %s", input);
        final Variable result = getGen().newVariable(LIRKind.combine(input));
        SPIRVUnary.Negate negateValue = new SPIRVUnary.Negate(LIRKind.combine(input), input);
        getGen().append(new SPIRVLIRStmt.AssignStmt(result, negateValue));
        return result;
    }

    @Override
    public Value emitMul(Value a, Value b, boolean setFlags) {
        Logger.traceBuildLIR(Logger.BACKEND.SPIRV, "[µInstructions] emitMul: %s * %s", a, b);
        SPIRVKind kind = (SPIRVKind) LIRKind.combine(a, b).getPlatformKind();
        SPIRVBinaryOp binaryOp;
        switch (kind) {
            case OP_TYPE_VECTOR3_INT_8:
            case OP_TYPE_VECTOR4_INT_8:
            case OP_TYPE_VECTOR2_INT_32:
            case OP_TYPE_VECTOR3_INT_32:
            case OP_TYPE_VECTOR4_INT_32:
            case OP_TYPE_VECTOR8_INT_32:
                return emitBinaryVectorAssign(SPIRVBinaryOp.MULT_INTEGER, LIRKind.combine(a, b), a, b);
            case OP_TYPE_VECTOR2_FLOAT_32:
            case OP_TYPE_VECTOR3_FLOAT_32:
            case OP_TYPE_VECTOR4_FLOAT_32:
            case OP_TYPE_VECTOR8_FLOAT_32:
            case OP_TYPE_VECTOR2_FLOAT_64:
            case OP_TYPE_VECTOR3_FLOAT_64:
            case OP_TYPE_VECTOR4_FLOAT_64:
            case OP_TYPE_VECTOR8_FLOAT_64:
                return emitBinaryVectorAssign(SPIRVBinaryOp.MULT_FLOAT, LIRKind.combine(a, b), a, b);
            case OP_TYPE_INT_8:
            case OP_TYPE_INT_64:
            case OP_TYPE_INT_32:
                binaryOp = SPIRVBinaryOp.MULT_INTEGER;
                break;
            case OP_TYPE_FLOAT_64:
            case OP_TYPE_FLOAT_32:
                binaryOp = SPIRVBinaryOp.MULT_FLOAT;
                break;
            default:
                throw new RuntimeException("Type not supported: " + kind);
        }
        return emitBinaryAssign(binaryOp, LIRKind.combine(a, b), a, b);
    }

    @Override
    public Value emitMulHigh(Value a, Value b) {
        return null;
    }

    @Override
    public Value emitUMulHigh(Value a, Value b) {
        return null;
    }

    @Override
    public Value emitDiv(Value a, Value b, LIRFrameState state) {
        Logger.traceBuildLIR(Logger.BACKEND.SPIRV, "[µInstructions] emitDiv: %s / %s", a, b);
        SPIRVKind kind = (SPIRVKind) LIRKind.combine(a, b).getPlatformKind();
        SPIRVBinaryOp binaryOp;
        switch (kind) {
            case OP_TYPE_VECTOR3_INT_8:
            case OP_TYPE_VECTOR4_INT_8:
            case OP_TYPE_VECTOR2_INT_32:
            case OP_TYPE_VECTOR3_INT_32:
            case OP_TYPE_VECTOR4_INT_32:
            case OP_TYPE_VECTOR8_INT_32:
                return emitBinaryVectorAssign(SPIRVBinaryOp.DIV_INTEGER, LIRKind.combine(a, b), a, b);
            case OP_TYPE_VECTOR2_FLOAT_32:
            case OP_TYPE_VECTOR3_FLOAT_32:
            case OP_TYPE_VECTOR4_FLOAT_32:
            case OP_TYPE_VECTOR8_FLOAT_32:
            case OP_TYPE_VECTOR2_FLOAT_64:
            case OP_TYPE_VECTOR3_FLOAT_64:
            case OP_TYPE_VECTOR4_FLOAT_64:
            case OP_TYPE_VECTOR8_FLOAT_64:
                return emitBinaryVectorAssign(SPIRVBinaryOp.DIV_FLOAT, LIRKind.combine(a, b), a, b);
            case OP_TYPE_INT_8:
            case OP_TYPE_INT_64:
            case OP_TYPE_INT_32:
                binaryOp = SPIRVBinaryOp.DIV_INTEGER;
                break;
            case OP_TYPE_FLOAT_64:
            case OP_TYPE_FLOAT_32:
                binaryOp = SPIRVBinaryOp.DIV_FLOAT;
                break;
            default:
                throw new RuntimeException("Type not supported: " + kind);
        }
        return emitBinaryAssign(binaryOp, LIRKind.combine(a, b), a, b);
    }

    @Override
    public Value emitRem(Value a, Value b, LIRFrameState state) {
        Logger.traceBuildLIR(Logger.BACKEND.SPIRV, "emitRem: %s MOD %s", a, b);
        return emitBinaryAssign(SPIRVBinaryOp.INTEGER_REM, LIRKind.combine(a, b), a, b);
    }

    @Override
    public Value emitUDiv(Value a, Value b, LIRFrameState state) {
        return null;
    }

    @Override
    public Value emitURem(Value a, Value b, LIRFrameState state) {
        return null;
    }

    @Override
    public Value emitNot(Value input) {
        throw new RuntimeException("Not supported");
    }

    @Override
    public Value emitAnd(Value a, Value b) {
        Logger.traceBuildLIR(Logger.BACKEND.SPIRV, "emitAnd: %s & %s", a, b);
        LIRKind lirKind = LIRKind.combine(a, b);
        final Variable result = getGen().newVariable(lirKind);
        SPIRVBinaryOp op = SPIRVBinaryOp.BITWISE_AND;
        getGen().append(new SPIRVLIRStmt.AssignStmt(result, genBinaryExpr(op, lirKind, a, b)));
        return result;
    }

    @Override
    public Value emitOr(Value a, Value b) {
        Logger.traceBuildLIR(Logger.BACKEND.SPIRV, "emitOR: %s | %s", a, b);
        LIRKind lirKind = LIRKind.combine(a, b);
        final Variable result = getGen().newVariable(lirKind);
        SPIRVBinaryOp op = SPIRVBinaryOp.BITWISE_OR;
        getGen().append(new SPIRVLIRStmt.AssignStmt(result, genBinaryExpr(op, lirKind, a, b)));
        return result;
    }

    @Override
    public Value emitXor(Value a, Value b) {
        Logger.traceBuildLIR(Logger.BACKEND.SPIRV, "emitXOR: %s ^ %s", a, b);
        LIRKind lirKind = LIRKind.combine(a, b);
        final Variable result = getGen().newVariable(lirKind);
        SPIRVBinaryOp op = SPIRVBinaryOp.BITWISE_XOR;
        getGen().append(new SPIRVLIRStmt.AssignStmt(result, genBinaryExpr(op, lirKind, a, b)));
        return result;
    }

    @Override
    public Value emitShl(Value a, Value b) {
        Logger.traceBuildLIR(Logger.BACKEND.SPIRV, "emitShl: %s << %s", a, b);
        LIRKind lirKind = LIRKind.combine(a, b);
        final Variable result = getGen().newVariable(lirKind);
        SPIRVBinaryOp op = SPIRVBinaryOp.BITWISE_LEFT_SHIFT;
        getGen().append(new SPIRVLIRStmt.AssignStmt(result, genBinaryExpr(op, lirKind, a, b)));
        return result;
    }

    @Override
    public Value emitShr(Value a, Value b) {
        Logger.traceBuildLIR(Logger.BACKEND.SPIRV, "emitShiftRight: %s >> %s", a, b);
        LIRKind lirKind = LIRKind.combine(a, b);
        final Variable result = getGen().newVariable(lirKind);
        SPIRVBinaryOp op = SPIRVBinaryOp.BITWISE_RIGHT_SHIFT;
        getGen().append(new SPIRVLIRStmt.AssignStmt(result, genBinaryExpr(op, lirKind, a, b)));
        return result;
    }

    @Override
    public Value emitUShr(Value a, Value b) {
        Logger.traceBuildLIR(Logger.BACKEND.SPIRV, "emitUnsignedShiftRight: %s >>> %s", a, b);
        LIRKind lirKind = LIRKind.combine(a, b);
        final Variable result = getGen().newVariable(lirKind);
        SPIRVBinaryOp op = SPIRVBinaryOp.BITWISE_UNSIGNED_RIGHT_SHIFT;
        getGen().append(new SPIRVLIRStmt.AssignStmt(result, genBinaryExpr(op, lirKind, a, b)));
        return result;
    }

    @Override
    public Value emitFloatConvert(FloatConvert op, Value inputVal) {
        return null;
    }

    @Override
    public Value emitReinterpret(LIRKind to, Value inputVal) {
        return null;
    }

    @Override
    public Value emitNarrow(Value inputVal, int bits) {
        Logger.traceBuildLIR(Logger.BACKEND.SPIRV, "emitNarrow: %s, %d", inputVal, bits);
        LIRKind lirKind = getGen().getLIRKindTool().getIntegerKind(bits);
        final Variable result = getGen().newVariable(lirKind);
        SPIRVUnary.SignNarrowValue signNarrowValue = new SPIRVUnary.SignNarrowValue(lirKind, inputVal, bits);
        getGen().append(new SPIRVLIRStmt.AssignStmt(result, signNarrowValue));
        return result;
    }

    @Override
    public Value emitSignExtend(Value inputVal, int fromBits, int toBits) {
        Logger.traceBuildLIR(Logger.BACKEND.SPIRV, "signExtend: %s , from %s to %s", inputVal, fromBits, toBits);
        LIRKind lirKind = getGen().getLIRKindTool().getIntegerKind(toBits);
        final Variable result = getGen().newVariable(lirKind);
        SPIRVUnary.SignExtend signExtend = new SPIRVUnary.SignExtend(lirKind, inputVal, fromBits, toBits);
        getGen().append(new SPIRVLIRStmt.AssignStmt(result, signExtend));
        return result;
    }

    @Override
    public Value emitZeroExtend(Value inputVal, int fromBits, int toBits) {
        Logger.traceBuildLIR(Logger.BACKEND.SPIRV, "emitZeroExtend: %s (from %d to %d)", inputVal, fromBits, toBits);
        SPIRVLIRKindTool kindTool = getGen().getLIRKindTool();
        SPIRVKind kind = (SPIRVKind) inputVal.getPlatformKind();

        LIRKind toKind;
        if (kind.isInteger()) {
            toKind = kindTool.getIntegerKind(toBits);
        } else if (kind.isFloatingPoint()) {
            toKind = kindTool.getFloatingKind(toBits);
        } else {
            throw new RuntimeException("Not supported kind: " + kind);
        }

        Variable result = getGen().newVariable(toKind);

        LIRKind lirKind = getGen().getLIRKindTool().getIntegerKind(toBits);
        SPIRVUnary.SignExtend signExtend = new SPIRVUnary.SignExtend(lirKind, inputVal, fromBits, toBits);
        getGen().append(new SPIRVLIRStmt.AssignStmt(result, signExtend));
        return result;
    }

    @Override
    public Value emitMathAbs(Value input) {
        return null;
    }

    @Override
    public Value emitMathSqrt(Value input) {
        return null;
    }

    @Override
    public Value emitBitCount(Value operand) {
        return null;
    }

    @Override
    public Value emitBitScanForward(Value operand) {
        return null;
    }

    @Override
    public Value emitBitScanReverse(Value operand) {
        return null;
    }

    private void emitLoad(AllocatableValue result, SPIRVAddressCast cast, MemoryAccess address) {
        Logger.traceBuildLIR(Logger.BACKEND.SPIRV, "emitLoad STMT: %s = (%s) %s", result.toString(), result.getPlatformKind().toString(), address.toString());
        getGen().append(new SPIRVLIRStmt.LoadStmt(result, cast, address));
    }

    private void emitLoadVectorType(AllocatableValue result, SPIRVAddressCast cast, MemoryAccess address) {
        Logger.traceBuildLIR(Logger.BACKEND.SPIRV, "emitLoadVector STMT: %s = (%s) %s", result.toString(), result.getPlatformKind().toString(), address.toString());
        getGen().append(new SPIRVLIRStmt.LoadVectorStmt(result, cast, address));
    }

    @Override
    public Variable emitLoad(LIRKind kind, Value address, LIRFrameState state) {
        Logger.traceBuildLIR(Logger.BACKEND.SPIRV, "emitLoad: %s <- %s with state:%s", kind, address, state);
        final Variable result = getGen().newVariable(kind);
        if (!(kind.getPlatformKind() instanceof SPIRVKind)) {
            throw new RuntimeException("invalid LIRKind");
        }

        SPIRVKind spirvKind = (SPIRVKind) kind.getPlatformKind();
        if (address instanceof SPIRVUnary.MemoryIndexedAccess) {
            SPIRVUnary.MemoryIndexedAccess indexedAccess = (SPIRVUnary.MemoryIndexedAccess) address;
            Logger.traceBuildLIR(Logger.BACKEND.SPIRV, "emit IndexedLoadMemAccess in address: " + address + "[ " + indexedAccess.getIndex() + "]");
            getGen().append(new SPIRVLIRStmt.IndexedLoadMemAccess(indexedAccess, result));
        } else if (address instanceof MemoryAccess) {
            SPIRVArchitecture.SPIRVMemoryBase base = ((MemoryAccess) (address)).getMemoryRegion();
            SPIRVAddressCast cast = new SPIRVAddressCast(address, base, kind);
            if (spirvKind.isVector()) {
                emitLoadVectorType(result, cast, (MemoryAccess) address);
            } else {
                emitLoad(result, cast, (MemoryAccess) address);
            }
        }

        return result;
    }

    @Override
    public Variable emitVolatileLoad(LIRKind kind, Value address, LIRFrameState state) {
        return null;
    }

    @Override
    public void emitStore(ValueKind<?> kind, Value address, Value input, LIRFrameState state) {
        Logger.traceBuildLIR(Logger.BACKEND.SPIRV, "emitStore: kind=%s, address=%s, input=%s", kind, address, input);
        guarantee(kind.getPlatformKind() instanceof SPIRVKind, "invalid LIRKind: %s", kind);
        SPIRVKind spirvKind = (SPIRVKind) kind.getPlatformKind();

        SPIRVUnary.AbstractMemoryAccess memAccess = null;

        if (address instanceof MemoryAccess) {
            memAccess = (MemoryAccess) address;
        } else if (address instanceof SPIRVUnary.MemoryIndexedAccess) {
            memAccess = (SPIRVUnary.MemoryIndexedAccess) address;
        }

        if (spirvKind.isVector()) {
            MemoryAccess memoryAccess2 = (MemoryAccess) address;
            SPIRVAddressCast cast = new SPIRVAddressCast(memAccess.getValue(), memoryAccess2.getMemoryRegion(), LIRKind.value(spirvKind));
            getGen().append(new SPIRVLIRStmt.StoreVectorStmt(cast, memoryAccess2, input));
        } else {
            if (memAccess != null) {
                if (address instanceof MemoryAccess) {
                    MemoryAccess memoryAccess2 = (MemoryAccess) address;
                    SPIRVAddressCast cast = new SPIRVAddressCast(memAccess.getValue(), memoryAccess2.getMemoryRegion(), LIRKind.value(spirvKind));
                    if (memoryAccess2.getIndex() == null) {
                        getGen().append(new SPIRVLIRStmt.StoreStmt(cast, memoryAccess2, input));
                    }
                } else if (address instanceof SPIRVUnary.MemoryIndexedAccess) {
                    SPIRVUnary.MemoryIndexedAccess indexedAccess = (SPIRVUnary.MemoryIndexedAccess) address;
                    getGen().append(new SPIRVLIRStmt.StoreIndexedMemAccess(indexedAccess, input));
                }

                AllocatableValue valueHolder = memAccess.assignedTo();
                if (valueHolder != null) {
                    getGen().append(new SPIRVLIRStmt.AssignStmt(valueHolder, input));
                }
            }
        }
    }

    @Override
    public void emitVolatileStore(ValueKind<?> kind, Value address, Value input, LIRFrameState state) {

    }

    private SPIRVKind getElementKind(SPIRVVectorElementSelect vector) {
        SPIRVKind spirvKind = (SPIRVKind) vector.getVector().getPlatformKind();
        return spirvKind.getElementKind();
    }

    private int getFloatTypeWidth(Value op1, Value op2, Value op3) {
        SPIRVKind kind1 = (SPIRVKind) op1.getPlatformKind();
        if (op1 instanceof SPIRVVectorElementSelect)
            kind1 = getElementKind((SPIRVVectorElementSelect) op1);

        SPIRVKind kind2 = (SPIRVKind) op2.getPlatformKind();
        if (op2 instanceof SPIRVVectorElementSelect)
            kind2 = getElementKind((SPIRVVectorElementSelect) op2);

        SPIRVKind kind3 = (SPIRVKind) op3.getPlatformKind();
        if (op3 instanceof SPIRVVectorElementSelect)
            kind3 = getElementKind((SPIRVVectorElementSelect) op3);

        SPIRVKind resultKindVector = kind1;
        if (kind1.getSizeInBytes() < kind2.getSizeInBytes()) {
            resultKindVector = kind2;
        }
        if (resultKindVector.getSizeInBytes() < kind3.getSizeInBytes()) {
            resultKindVector = kind3;
        }
        return resultKindVector.getSizeInBytes() * 8;
    }

    public Value emitFMAInstruction(Value op1, Value op2, Value op3) {
        LIRKind resultKind = LIRKind.combine(op1, op2, op3);

        if ((op1 instanceof SPIRVVectorElementSelect) || (op2 instanceof SPIRVVectorElementSelect) || (op3 instanceof SPIRVVectorElementSelect)) {
            // FMA are composed of type float (16, 32, 64) or vector types. However, if the
            // inputs are of type VectorSelect, that means we are selecting one of the
            // components of the vector (float).
            int numBits = getFloatTypeWidth(op1, op2, op3);
            resultKind = getGen().getLIRKindTool().getFloatingKind(numBits);
        }

        Variable result = getGen().newVariable(resultKind);
        getGen().append(new SPIRVLIRStmt.AssignStmt(result, new SPIRVTernary.TernaryIntrinsic(SPIRVUnary.Intrinsic.OpenCLExtendedIntrinsic.FMA, resultKind, op1, op2, op3)));
        return result;
    }
}
