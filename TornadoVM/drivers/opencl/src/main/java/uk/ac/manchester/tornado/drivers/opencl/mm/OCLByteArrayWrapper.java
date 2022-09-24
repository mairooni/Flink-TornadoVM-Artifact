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
package uk.ac.manchester.tornado.drivers.opencl.mm;

import jdk.vm.ci.meta.JavaKind;

import uk.ac.manchester.tornado.drivers.opencl.OCLDeviceContext;

public class OCLByteArrayWrapper extends OCLArrayWrapper<byte[]> {

    public OCLByteArrayWrapper(OCLDeviceContext device, long batchSize) {
        this(device, false, batchSize);
    }

    public OCLByteArrayWrapper(OCLDeviceContext device, boolean isFinal, long batchSize) {
        super(device, JavaKind.Byte, isFinal, batchSize);
    }

    @Override
    protected int readArrayData(long bufferId, long offset, long bytes, byte[] value, long hostOffset, int[] waitEvents) {
        int res = deviceContext.readBuffer(bufferId, offset, bytes, value, hostOffset, waitEvents);
        return res;
    }

    @Override
    protected void writeArrayData(long bufferId, long offset, long bytes, byte[] value, long hostOffset, int[] waitEvents) {
        deviceContext.writeBuffer(bufferId, offset, bytes, value, hostOffset, waitEvents);
    }

    @Override
    protected int enqueueReadArrayData(long bufferId, long offset, long bytes, byte[] value, long hostOffset, int[] waitEvents) {
        return deviceContext.enqueueReadBuffer(bufferId, offset, bytes, value, hostOffset, waitEvents);
    }

    @Override
    protected int enqueueWriteArrayData(long bufferId, long offset, long bytes, byte[] value, long hostOffset, int[] waitEvents) {
        return deviceContext.enqueueWriteBuffer(bufferId, offset, bytes, value, hostOffset, waitEvents);
    }

}
