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
package uk.ac.manchester.tornado.drivers.opencl.graal;

import static jdk.vm.ci.code.MemoryBarriers.LOAD_STORE;
import static jdk.vm.ci.code.MemoryBarriers.STORE_STORE;
import static uk.ac.manchester.tornado.api.exceptions.TornadoInternalError.shouldNotReachHere;
import static uk.ac.manchester.tornado.drivers.opencl.graal.asm.OCLAssemblerConstants.ATOMICS_REGION_NAME;
import static uk.ac.manchester.tornado.drivers.opencl.graal.asm.OCLAssemblerConstants.CONSTANT_REGION_NAME;
import static uk.ac.manchester.tornado.drivers.opencl.graal.asm.OCLAssemblerConstants.FRAME_BASE_NAME;
import static uk.ac.manchester.tornado.drivers.opencl.graal.asm.OCLAssemblerConstants.HEAP_REF_NAME;
import static uk.ac.manchester.tornado.drivers.opencl.graal.asm.OCLAssemblerConstants.LOCAL_REGION_NAME;
import static uk.ac.manchester.tornado.drivers.opencl.graal.asm.OCLAssemblerConstants.PRIVATE_REGION_NAME;

import java.nio.ByteOrder;

import jdk.vm.ci.code.Architecture;
import jdk.vm.ci.code.Register.RegisterCategory;
import jdk.vm.ci.meta.JavaKind;
import jdk.vm.ci.meta.PlatformKind;
import uk.ac.manchester.tornado.drivers.opencl.graal.lir.OCLKind;
import uk.ac.manchester.tornado.drivers.opencl.graal.meta.OCLMemorySpace;

public class OCLArchitecture extends Architecture {

    public static final RegisterCategory OCL_ABI = new RegisterCategory("abi");

    public static class OCLRegister {

        // FIXME: THis field can be removed
        public final int number;
        public final String name;
        public final OCLKind lirKind;

        public OCLRegister(int number, String name, OCLKind lirKind) {
            this.number = number;
            this.name = name;
            this.lirKind = lirKind;

        }

        public String getDeclaration() {
            return String.format("%s %s", lirKind.toString(), name);
        }

        public String getName() {
            return name;
        }
    }

    public static class OCLMemoryBase extends OCLRegister {

        public final OCLMemorySpace memorySpace;

        public OCLMemoryBase(int number, String name, OCLMemorySpace memorySpace, OCLKind kind) {
            super(number, name, kind);
            this.memorySpace = memorySpace;
        }

        @Override
        public String getDeclaration() {
            return String.format("%s %s *%s", memorySpace.name(), lirKind.toString(), name);
        }

    }

    public static final OCLMemoryBase globalSpace = new OCLMemoryBase(0, HEAP_REF_NAME, OCLMemorySpace.GLOBAL, OCLKind.UCHAR);
    public static OCLRegister sp;
    public static final OCLMemoryBase constantSpace = new OCLMemoryBase(2, CONSTANT_REGION_NAME, OCLMemorySpace.CONSTANT, OCLKind.UCHAR);
    public static final OCLMemoryBase localSpace = new OCLMemoryBase(3, LOCAL_REGION_NAME, OCLMemorySpace.LOCAL, OCLKind.UCHAR);
    public static final OCLMemoryBase privateSpace = new OCLMemoryBase(4, PRIVATE_REGION_NAME, OCLMemorySpace.PRIVATE, OCLKind.UCHAR);
    public static final OCLMemoryBase atomicSpace = new OCLMemoryBase(5, ATOMICS_REGION_NAME, OCLMemorySpace.GLOBAL, OCLKind.INT);

    public static OCLRegister[] abiRegisters;

    public OCLArchitecture(final OCLKind wordKind, final ByteOrder byteOrder) {
        super("Tornado OpenCL", wordKind, byteOrder, false, null, LOAD_STORE | STORE_STORE, 0, 0);
        sp = new OCLRegister(1, FRAME_BASE_NAME, wordKind);
        abiRegisters = new OCLRegister[] { globalSpace, sp, constantSpace, localSpace, atomicSpace };
    }

    @Override
    public PlatformKind getPlatformKind(JavaKind javaKind) {
        OCLKind oclKind = OCLKind.ILLEGAL;
        switch (javaKind) {
            case Boolean:
                oclKind = OCLKind.BOOL;
                break;
            case Byte:
                oclKind = OCLKind.CHAR;
                break;
            case Short:
                oclKind = (javaKind.isUnsigned()) ? OCLKind.USHORT : OCLKind.SHORT;
                break;
            case Char:
                oclKind = OCLKind.USHORT;
                break;
            case Int:
                oclKind = (javaKind.isUnsigned()) ? OCLKind.UINT : OCLKind.INT;
                break;
            case Long:
                oclKind = (javaKind.isUnsigned()) ? OCLKind.ULONG : OCLKind.LONG;
                break;
            case Float:
                oclKind = OCLKind.FLOAT;
                break;
            case Double:
                oclKind = OCLKind.DOUBLE;
                break;
            case Object:
                oclKind = (OCLKind) getWordKind();
                break;
            case Void:
            case Illegal:
                oclKind = OCLKind.ILLEGAL;
                break;
            default:
                shouldNotReachHere("illegal java type for %s", javaKind.name());
        }

        return oclKind;
    }

    @Override
    public int getReturnAddressSize() {
        return this.getWordSize();
    }

    @Override
    public boolean canStoreValue(RegisterCategory category, PlatformKind platformKind) {
        return false;
    }

    @Override
    public PlatformKind getLargestStorableKind(RegisterCategory category) {
        return OCLKind.LONG;
    }

    public String getABI() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < abiRegisters.length; i++) {
            sb.append(abiRegisters[i].getDeclaration());
            if (i < abiRegisters.length - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    public String getCallingConvention() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < abiRegisters.length; i++) {
            sb.append(abiRegisters[i].getName());
            if (i < abiRegisters.length - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
