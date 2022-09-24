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
package uk.ac.manchester.tornado.drivers.opencl.runtime;

public class OCLMemoryRegions {

    private int globalSize;
    private int constantSize;
    private int localSize;
    private int privateSize;
    private byte[] constantData;

    public OCLMemoryRegions() {
        this.globalSize = 0;
        this.constantSize = 0;
        this.localSize = 0;
        this.privateSize = 0;
        this.constantData = null;
    }

    public void allocGlobal(int size) {
        this.globalSize = size;
    }

    public void allocLocal(int size) {
        this.localSize = size;
    }

    public void allocPrivate(int size) {
        this.privateSize = size;
    }

    public void allocConstant(int size) {
        this.constantSize = size;
        constantData = new byte[size];
    }

    public int getGlobalSize() {
        return globalSize;
    }

    public int getConstantSize() {
        return constantSize;
    }

    public int getLocalSize() {
        return localSize;
    }

    public int getPrivateSize() {
        return privateSize;
    }

    public byte[] getConstantData() {
        return constantData;
    }

}
