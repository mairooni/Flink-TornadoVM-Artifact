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
package uk.ac.manchester.tornado.drivers.opencl;

import uk.ac.manchester.tornado.api.TornadoTargetDevice;
import uk.ac.manchester.tornado.api.common.Access;
import uk.ac.manchester.tornado.api.exceptions.TornadoRuntimeException;
import uk.ac.manchester.tornado.drivers.opencl.graal.OCLInstalledCode;
import uk.ac.manchester.tornado.drivers.opencl.runtime.OCLTornadoDevice;
import uk.ac.manchester.tornado.drivers.opencl.virtual.VirtualDeviceDescriptor;
import uk.ac.manchester.tornado.drivers.opencl.virtual.VirtualJSONParser;
import uk.ac.manchester.tornado.drivers.opencl.virtual.VirtualOCLPlatform;
import uk.ac.manchester.tornado.runtime.common.CallStack;
import uk.ac.manchester.tornado.runtime.common.DeviceObjectState;
import uk.ac.manchester.tornado.runtime.common.Tornado;
import uk.ac.manchester.tornado.runtime.tasks.GlobalObjectState;
import uk.ac.manchester.tornado.runtime.tasks.meta.TaskMetaData;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static uk.ac.manchester.tornado.runtime.common.Tornado.getProperty;
import static uk.ac.manchester.tornado.runtime.common.TornadoOptions.VIRTUAL_DEVICE_ENABLED;

public class OpenCL {

    public static final String OPENCL_JNI_LIBRARY = "tornado-opencl";

    private static boolean initialised = false;

    private static final List<TornadoPlatform> platforms = new ArrayList<>();

    public static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    public static final int CL_TRUE = 1;
    public static final int CL_FALSE = 0;

    static {
        if (VIRTUAL_DEVICE_ENABLED) {
            initializeVirtual();
        } else {
            try {
                // Loading JNI OpenCL library
                System.loadLibrary(OpenCL.OPENCL_JNI_LIBRARY);
            } catch (final UnsatisfiedLinkError e) {
                throw e;
            }

            try {
                initialise();
            } catch (final TornadoRuntimeException e) {
                e.printStackTrace();
            }

            // add a shutdown hook to free-up all OpenCL resources on VM exit
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    setName("OpenCL-Cleanup-Thread");
                    OpenCL.cleanup();
                }
            });
        }
    }

    native static boolean registerCallback();

    native static int clGetPlatformCount();

    native static int clGetPlatformIDs(long[] platformIds);

    public static void cleanup() {
        if (initialised) {
            for (final TornadoPlatform platform : platforms) {
                platform.cleanup();
            }
        }
    }

    public static TornadoPlatform getPlatform(int index) {
        return platforms.get(index);
    }

    public static int getNumPlatforms() {
        return platforms.size();
    }

    private static void initializeVirtual() {
        if (!initialised) {
            VirtualDeviceDescriptor info = VirtualJSONParser.getDeviceDescriptor();

            VirtualOCLPlatform platform = new VirtualOCLPlatform(info);
            platforms.add(platform);

            initialised = true;
        }
    }

    public static void initialise() {
        if (!initialised) {
            try {
                int numPlatforms = clGetPlatformCount();
                long[] ids = new long[numPlatforms];
                clGetPlatformIDs(ids);

                for (int i = 0; i < ids.length; i++) {
                    OCLPlatform platform = new OCLPlatform(i, ids[i]);
                    platforms.add(platform);
                }

            } catch (final Exception exc) {
                exc.printStackTrace();
                throw new TornadoRuntimeException("Problem with OpenCL bindings");
            } catch (final Error err) {
                err.printStackTrace();
                throw new TornadoRuntimeException("Error with OpenCL bindings");
            }
            initialised = true;
        }
    }

    public static OCLTornadoDevice defaultDevice() {
        final int platformIndex = Integer.parseInt(Tornado.getProperty("tornado.opencl.platform", "0"));
        final int deviceIndex = Integer.parseInt(Tornado.getProperty("tornado.opencl.device", "0"));
        return new OCLTornadoDevice(platformIndex, deviceIndex);
    }

    /**
     * Execute an OpenCL code compiled by Tornado on the target device
     * 
     * @param tornadoDevice
     *            OpenCL device to run the application.
     * @param openCLCode
     *            OpenCL code to run.
     * @param taskMeta
     *            TaskMetadata.
     * @param accesses
     *            Access of each parameter
     * @param parameters
     *            List of parameters.
     * 
     */
    public static void run(OCLTornadoDevice tornadoDevice, OCLInstalledCode openCLCode, TaskMetaData taskMeta, Access[] accesses, Object... parameters) {
        if (parameters.length != accesses.length) {
            throw new TornadoRuntimeException("[ERROR] Accesses and objects array should match in size");
        }

        // Copy-in variables
        ArrayList<DeviceObjectState> states = new ArrayList<>();
        for (int i = 0; i < accesses.length; i++) {
            Access access = accesses[i];
            Object object = parameters[i];

            GlobalObjectState globalState = new GlobalObjectState();
            DeviceObjectState deviceState = globalState.getDeviceState(tornadoDevice);

            switch (access) {
                case READ_WRITE:
                case READ:
                    tornadoDevice.ensurePresent(object, deviceState, null, 0, 0);
                    break;
                case WRITE:
                    tornadoDevice.ensureAllocated(object, 0, deviceState);
                default:
                    break;
            }
            states.add(deviceState);
        }

        // Create stack
        final int numArgs = parameters.length;
        CallStack stack = tornadoDevice.createStack(numArgs);

        // Fill header of call stack with empty values
        stack.setHeader(new HashMap<>());

        // Pass arguments to the call stack
        for (int i = 0; i < numArgs; i++) {
            stack.push(parameters[i], states.get(i));
        }

        // Run the code
        openCLCode.launchWithoutDependencies(stack, null, taskMeta, 0);

        // Obtain the result
        for (int i = 0; i < accesses.length; i++) {
            Access access = accesses[i];
            switch (access) {
                case READ_WRITE:
                case WRITE:
                    Object object = parameters[i];
                    DeviceObjectState deviceState = states.get(i);
                    tornadoDevice.streamOutBlocking(object, 0, deviceState, null);
                default:
                    break;
            }
        }
    }

    public static List<TornadoPlatform> platforms() {
        return platforms;
    }

    public static void exploreAllPlatforms() {
        for (int platformIndex = 0; platformIndex < platforms.size(); platformIndex++) {
            final TornadoPlatform platform = platforms.get(platformIndex);
            System.out.printf("[%d]: platform: %s\n", platformIndex, platform.getName());
            final OCLExecutionEnvironment context = platform.createContext();
            for (int deviceIndex = 0; deviceIndex < context.getNumDevices(); deviceIndex++) {
                OCLDeviceContext deviceContext = (OCLDeviceContext) context.createDeviceContext(deviceIndex);
                System.out.printf("\t[%d:%d] device: %s\n", platformIndex, deviceIndex, deviceContext.getDeviceName());
            }
        }
    }

    public static TornadoTargetDevice getDevice(int platformIndex, int deviceIndex) {
        final TornadoPlatform platform = platforms.get(platformIndex);
        OCLDeviceContext deviceContext = (OCLDeviceContext) platform.createContext().createDeviceContext(deviceIndex);
        return deviceContext.getDevice();
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            exploreAllPlatforms();
        } else if (args.length == 2) {
            final int platformIndex = Integer.parseInt(args[0]);
            final int deviceIndex = Integer.parseInt(args[1]);
            TornadoTargetDevice device = getDevice(platformIndex, deviceIndex);
            System.out.println(device.getDeviceInfo());
        } else {
            System.out.println("usage: OpenCL <platform> <device>");
        }
    }
}
