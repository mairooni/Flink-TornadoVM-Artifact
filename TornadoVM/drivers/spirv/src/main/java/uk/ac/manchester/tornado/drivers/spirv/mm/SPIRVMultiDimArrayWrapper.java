/*
 * This file is part of Tornado: A heterogeneous programming framework:
 * https://github.com/beehive-lab/tornadovm
 *
 * Copyright (c) 2021, APT Group, Department of Computer Science,
 * School of Engineering, The University of Manchester. All rights reserved.
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
package uk.ac.manchester.tornado.drivers.spirv.mm;

import static uk.ac.manchester.tornado.runtime.common.Tornado.fatal;

import java.lang.reflect.Array;
import java.util.function.Function;

import jdk.vm.ci.meta.JavaKind;
import uk.ac.manchester.tornado.api.exceptions.TornadoMemoryException;
import uk.ac.manchester.tornado.api.exceptions.TornadoOutOfMemoryException;
import uk.ac.manchester.tornado.drivers.spirv.SPIRVDeviceContext;

public class SPIRVMultiDimArrayWrapper<T, E> extends SPIRVArrayWrapper<T> {

    private Function<SPIRVDeviceContext, ? extends SPIRVArrayWrapper<E>> innerWrapperFactory;
    private SPIRVLongArrayWrapper tableWrapper;
    private long[] addresses;
    private SPIRVArrayWrapper<E>[] wrappers;
    private SPIRVDeviceContext deviceContext;

    public SPIRVMultiDimArrayWrapper(SPIRVDeviceContext deviceContext, Function<SPIRVDeviceContext, ? extends SPIRVArrayWrapper<E>> innerWrapperFactory, long batchSize) {
        this(deviceContext, innerWrapperFactory, false, batchSize);
    }

    private SPIRVMultiDimArrayWrapper(SPIRVDeviceContext deviceContext, Function<SPIRVDeviceContext, ? extends SPIRVArrayWrapper<E>> innerWrapperFactory, boolean isFinal, long batchSize) {
        super(deviceContext, JavaKind.Object, isFinal, batchSize);
        this.deviceContext = deviceContext;
        this.innerWrapperFactory = innerWrapperFactory;
        this.tableWrapper = new SPIRVLongArrayWrapper(deviceContext, false, batchSize);
    }

    @Override
    public long toRelativeAddress() {
        return tableWrapper.toRelativeAddress();
    }

    @Override
    public long toBuffer() {
        return tableWrapper.toBuffer();
    }

    @Override
    public long toAbsoluteAddress() {
        return tableWrapper.toAbsoluteAddress();
    }

    @Override
    public void invalidate() {
        tableWrapper.invalidate();
    }

    @Override
    public boolean isValid() {
        return tableWrapper.isValid();
    }

    @Override
    public long getBufferOffset() {
        return tableWrapper.getBufferOffset();
    }

    @Override
    public long size() {
        return tableWrapper.size();
    }

    private E[] innerCast(T value) {
        return (E[]) value;
    }

    private void allocateElements(T values, long batchSize) {
        final E[] elements = innerCast(values);
        try {
            for (int i = 0; i < elements.length; i++) {
                wrappers[i] = innerWrapperFactory.apply(deviceContext);
                wrappers[i].allocate(elements[i], batchSize);
                addresses[i] = deviceContext.useRelativeAddresses() ? wrappers[i].toRelativeAddress() : wrappers[i].toAbsoluteAddress();
            }
        } catch (TornadoOutOfMemoryException | TornadoMemoryException e) {
            fatal("OOM: multi-dim array: %s", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public void allocate(Object value, long batchSize) throws TornadoOutOfMemoryException, TornadoMemoryException {
        if (batchSize > 0) {
            throw new TornadoMemoryException("[ERROR] BatchSize Allocation currently not supported. BatchSize = " + batchSize + " (bytes)");
        }

        if (Array.getLength(value) < 0) {
            throw new TornadoMemoryException("[ERROR] Bytes Allocated < 0: " + Array.getLength(value));
        }
        addresses = new long[Array.getLength(value)];
        wrappers = new SPIRVArrayWrapper[Array.getLength(value)];
        tableWrapper.allocate(addresses, batchSize);
        allocateElements((T) value, batchSize);
    }

    private int readElements(T values) {
        final E[] elements = innerCast(values);
        // XXX: Offset is 0
        for (int i = 0; i < elements.length; i++) {
            wrappers[i].enqueueRead(elements[i], 0, null, false);
        }
        deviceContext.enqueueBarrier(deviceContext.getDeviceIndex());
        return 0;
    }

    @Override
    protected int readArrayData(long bufferId, long offset, long bytes, T value, long hostOffset, int[] waitEvents) {
        return readElements(value);
    }

    private int writeElements(T values) {
        final E[] elements = innerCast(values);
        for (int i = 0; i < elements.length; i++) {
            wrappers[i].enqueueWrite(elements[i], 0, 0, null, false);
        }
        deviceContext.enqueueBarrier(deviceContext.getDeviceIndex());
        return 0;
    }

    @Override
    protected void writeArrayData(long bufferId, long offset, long bytes, T value, long hostOffset, int[] waitEvents) {
        if (hostOffset > 0) {
            System.out.println("[WARNING] writing in offset 0");
        }
        tableWrapper.enqueueWrite(addresses, 0, 0, null, false);
        writeElements(value);
    }

    @Override
    protected int enqueueReadArrayData(long bufferId, long offset, long bytes, T value, long hostOffset, int[] waitEvents) {
        return readElements(value);
    }

    @Override
    protected int enqueueWriteArrayData(long bufferId, long offset, long bytes, T value, long hostOffset, int[] waitEvents) {
        if (hostOffset > 0) {
            System.out.println("[WARNING] writing in offset 0");
        }
        tableWrapper.enqueueWrite(addresses, 0, 0, null, false);
        return writeElements(value);
    }
}
