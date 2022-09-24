/*
 * This file is part of Tornado: A heterogeneous programming framework:
 * https://github.com/beehive-lab/tornadovm
 *
 * Copyright (c) 2020, APT Group, Department of Computer Science,
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

package uk.ac.manchester.tornado.drivers.ptx.graal.asm;

public class PTXAssemblerConstants {

    public static String VPRINTF_PROTOTYPE = ".extern .func (.param .b32 status) vprintf (.param .b64 format, .param .b64 valist);";

    public static final String REG = "reg";
    public static final String VECTOR = "v";

    public static final String CONVERT = "cvt";
    public static final String CONVERT_ADDRESS = "cvta";
    public static final String MOVE = "mov";
    public static final String TEST_NUMBER = "testp.number";
    public static final String TEST_NORMAL = "testp.normal";
    public static final String TEST_SUBNORMAL = "testp.subnormal";

    public static final String HEAP_PTR_NAME = "heap_pointer";
    public static final String STACK_PTR_NAME = "stack_pointer";
    public static final String GLOBAL_MEM_MODIFIER = "global";
    public static final String PARAM_MEM_MODIFIER = "param";
    public static final String SHARED_MEM_MODIFIER = "shared";
    public static final String LOCAL_MEM_MODIFIER = "local";

    public static final String COMPUTE_VERSION = ".version";
    public static final String TARGET_ARCH = ".target";
    public static final String ADDRESS_HEADER = ".address_size";
    public static final String EXTERNALLY_VISIBLE = ".visible";
    public static final String KERNEL_ENTRYPOINT = ".entry";

    public static final String ROUND_NEAREST_EVEN = "rn";
    public static final String ROUND_NEAREST_EVEN_INTEGER = "rni";
    public static final String ROUND_TOWARD_ZERO_INTEGER = "rzi";
    public static final String ROUND_NEGATIVE_INFINITY_INTEGER = "rmi";

    public static final String TAB = "\t";
    public static final String COMMA = ",";
    public static final String STMT_DELIMITER = ";";
    public static final String EOL = "\n";
    public static final String DOT = ".";
    public static final String COLON = ":";
    public static final String OP_GUARD = "@";
    public static final String NEGATION = "!";
    public static final String SPACE = " ";
    public static final String ASSIGN = "=";

    public static final String SQUARE_BRACKETS_OPEN = "[";
    public static final String SQUARE_BRACKETS_CLOSE = "]";
    public static final String CURLY_BRACKETS_OPEN = "{";
    public static final String CURLY_BRACKETS_CLOSE = "}";
    public static final String ROUND_BRACKETS_OPEN = "(";
    public static final String ROUND_BRACKETS_CLOSE = ")";

    public static final String BRANCH = "bra";
    public static final String UNI = "uni";

    public static final String CALL = "call";

    public static final int STACK_BASE_OFFSET = 3;
}
