/*
 * This file is part of Tornado: A heterogeneous programming framework: 
 * https://github.com/beehive-lab/tornadovm
 *
 * Copyright (c) 2013-2020, APT Group, Department of Computer Science,
 * The University of Manchester. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * GNU Classpath is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2, or (at your option)
 * any later version.
 * 
 * GNU Classpath is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with GNU Classpath; see the file COPYING.  If not, write to the
 * Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 *
 * Linking this library statically or dynamically with other modules is
 * making a combined work based on this library.  Thus, the terms and
 * conditions of the GNU General Public License cover the whole
 * combination.
 * 
 * As a special exception, the copyright holders of this library give you
 * permission to link this library with independent modules to produce an
 * executable, regardless of the license terms of these independent
 * modules, and to copy and distribute the resulting executable under
 * terms of your choice, provided that you also meet, for each linked
 * independent module, the terms and conditions of the license of that
 * module.  An independent module is a module which is not derived from
 * or based on this library.  If you modify this library, you may extend
 * this exception to your version of the library, but you are not
 * obligated to do so.  If you do not wish to do so, delete this
 * exception statement from your version.
 *
 */
package uk.ac.manchester.tornado.api.exceptions;

import static uk.ac.manchester.tornado.api.exceptions.TornadoInternalError.shouldNotReachHere;

public class Debug {

    /**
     * prints a message from the zeroth thread
     *
     * @param msg
     *            format string as per OpenCL spec
     * @param args
     *            arguments to format
     */
    public static void tprintf(String msg, Object... args) {
        shouldNotReachHere();
    }

    /**
     * prints a message from the selected thread [id, 0, 0]
     *
     * @param id
     *            selected thread id
     * @param msg
     *            format string as per OpenCL spec
     * @param args
     *            arguments to format
     */
    public static void tprintf(int id, String msg, Object... args) {
        shouldNotReachHere();
    }

    /**
     * prints a message from the selected thread [id0, id1, 0]
     *
     * @param id0
     *            selected thread id
     * @param id1
     *            selected thread id
     * @param msg
     *            format string as per OpenCL spec
     * @param args
     *            arguments to format
     */
    public static void tprintf(int id0, int id1, String msg, Object... args) {
        shouldNotReachHere();
    }

    /**
     * prints a message from the selected thread [id0, id1, id2]
     *
     * @param id0
     *            selected thread id
     * @param id1
     *            selected thread id
     * @param id2
     *            selected thread id
     * @param msg
     *            format string as per OpenCL spec
     * @param args
     *            arguments to format
     */
    public static void tprintf(int id0, int id1, int id2, String msg, Object... args) {
        shouldNotReachHere();
    }

    /**
     * conditionally prints a message from any thread where cond evaluatest to
     * true
     *
     * @param cond
     *            condition to evaluate
     * @param msg
     *            format string as per OpenCL spec
     * @param args
     *            arguments to format
     */
    public static void printf(boolean cond, String msg, Object... args) {
        shouldNotReachHere();
    }

    /**
     * prints a message from all threads
     *
     * @param msg
     *            format string as per OpenCL spec
     * @param args
     *            arguments to format
     */
    public static void printf(String msg, Object... args) {
        shouldNotReachHere();
    }

}
