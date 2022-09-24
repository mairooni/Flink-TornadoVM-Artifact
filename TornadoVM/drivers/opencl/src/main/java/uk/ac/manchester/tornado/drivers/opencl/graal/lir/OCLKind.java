/*
 * Copyright (c) 2020, APT Group, Department of Computer Science,
 * School of Engineering, The University of Manchester. All rights reserved.
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

import static uk.ac.manchester.tornado.api.exceptions.TornadoInternalError.guarantee;
import static uk.ac.manchester.tornado.api.exceptions.TornadoInternalError.shouldNotReachHere;
import static uk.ac.manchester.tornado.api.exceptions.TornadoInternalError.unimplemented;

import jdk.vm.ci.meta.JavaConstant;
import jdk.vm.ci.meta.JavaKind;
import jdk.vm.ci.meta.PlatformKind;
import jdk.vm.ci.meta.ResolvedJavaType;
import uk.ac.manchester.tornado.api.type.annotations.Vector;
import uk.ac.manchester.tornado.drivers.opencl.graal.asm.OCLAssembler;

public enum OCLKind implements PlatformKind {

    // @formatter:off
    ATOMIC_ADD_INT(4, java.lang.Integer.TYPE),  
    ATOMIC_ADD_FLOAT(4, java.lang.Float.TYPE),
    ATOMIC_SUB_INT(4, java.lang.Integer.TYPE),
    ATOMIC_MUL_INT(4, java.lang.Integer.TYPE),
	ATOMIC_ADD_LONG(8, java.lang.Long.TYPE),
    BOOL(1, java.lang.Boolean.TYPE),
    CHAR(1, java.lang.Byte.TYPE),
    UCHAR(1, null),
    SHORT(2, java.lang.Short.TYPE),
    USHORT(2, null),
    INT(4, java.lang.Integer.TYPE),
    UINT(4, null),
    LONG(8, java.lang.Long.TYPE),
    ULONG(8, null),
    HALF(2, null),
    FLOAT(4, java.lang.Float.TYPE),
    DOUBLE(8, java.lang.Double.TYPE),
    CHAR2(2, null, CHAR),
    UCHAR2(2, null, UCHAR),
    SHORT2(2, uk.ac.manchester.tornado.api.collections.types.Short2.TYPE, SHORT),
    USHORT2(2, null, USHORT),
    INT2(2, uk.ac.manchester.tornado.api.collections.types.Int2.TYPE, INT),
    UINT2(2, null, UINT),
    LONG2(2, null, LONG),
    ULONG2(2, null, ULONG),
    FLOAT2(2, uk.ac.manchester.tornado.api.collections.types.Float2.TYPE, FLOAT),
    DOUBLE2(2, uk.ac.manchester.tornado.api.collections.types.Double2.TYPE, DOUBLE),
    CHAR3(3, uk.ac.manchester.tornado.api.collections.types.Byte3.TYPE, CHAR),
    UCHAR3(3, null, UCHAR),
    SHORT3(3, uk.ac.manchester.tornado.api.collections.types.Short3.TYPE, SHORT),
    USHORT3(3, null, USHORT),
    INT3(3, uk.ac.manchester.tornado.api.collections.types.Int3.TYPE, INT),
    UINT3(3, null, UINT),
    LONG3(3, null, LONG),
    ULONG3(3, null, ULONG),
    FLOAT3(3, uk.ac.manchester.tornado.api.collections.types.Float3.TYPE, FLOAT),
    DOUBLE3(3, uk.ac.manchester.tornado.api.collections.types.Double3.TYPE, DOUBLE),
    CHAR4(4, uk.ac.manchester.tornado.api.collections.types.Byte4.TYPE, CHAR),
    UCHAR4(4, null, UCHAR),
    SHORT4(4, null, SHORT),
    USHORT4(4, null, USHORT),
    INT4(4, uk.ac.manchester.tornado.api.collections.types.Int4.TYPE, INT),
    UINT4(4, null, UINT),
    LONG4(4, null, LONG),
    ULONG4(4, null, ULONG),
    FLOAT4(4, uk.ac.manchester.tornado.api.collections.types.Float4.TYPE, FLOAT),
    DOUBLE4(4, uk.ac.manchester.tornado.api.collections.types.Double4.TYPE, DOUBLE),
    CHAR8(8, null, CHAR),
    UCHAR8(8, null, UCHAR),
    SHORT8(8, null, SHORT),
    USHORT8(8, null, USHORT),
    INT8(8, uk.ac.manchester.tornado.api.collections.types.Int8.TYPE, INT),
    UINT8(8, null, UINT),
    LONG8(8, null, LONG),
    ULONG8(8, null, ULONG),
    FLOAT8(8, uk.ac.manchester.tornado.api.collections.types.Float8.TYPE, FLOAT),
    DOUBLE8(8, uk.ac.manchester.tornado.api.collections.types.Double8.TYPE, DOUBLE),        
    CHAR16(16, null, CHAR),
    UCHAR16(16, null, UCHAR),
    SHORT16(16, null, SHORT),
    USHORT16(16, null, USHORT),
    INT16(16, null, INT),
    UINT16(16, null, UINT),
    LONG16(16, null, LONG),
    ULONG16(16, null, ULONG),
    FLOAT16(16, null, FLOAT),
    DOUBLE16(16, null, DOUBLE),
    ILLEGAL(0, null),
    
    INTEGER_ATOMIC_JAVA(4, java.util.concurrent.atomic.AtomicInteger.class);
    // @formatter:on

    public static OCLKind fromResolvedJavaType(ResolvedJavaType type) {
        if (!type.isArray()) {
            for (OCLKind k : OCLKind.values()) {
                if (k.javaClass != null && (k.javaClass.getSimpleName().equalsIgnoreCase(type.getJavaKind().name()) || k.javaClass.getSimpleName().equals(type.getUnqualifiedName()))) {
                    return k;
                }
            }
        }
        return ILLEGAL;
    }

    public static OCLKind fromResolvedJavaKind(JavaKind javaKind) {
        for (OCLKind k : OCLKind.values()) {
            if (k.javaClass != null && k.javaClass.getSimpleName().equalsIgnoreCase(javaKind.name())) {
                return k;
            }
        }
        return ILLEGAL;
    }

    public static OCLAssembler.OCLBinaryTemplate resolvePrivateTemplateType(JavaKind type) {
        if (type == JavaKind.Int) {
            return OCLAssembler.OCLBinaryTemplate.NEW_PRIVATE_INT_ARRAY;
        } else if (type == JavaKind.Double) {
            return OCLAssembler.OCLBinaryTemplate.NEW_PRIVATE_DOUBLE_ARRAY;
        } else if (type == JavaKind.Float) {
            return OCLAssembler.OCLBinaryTemplate.NEW_PRIVATE_FLOAT_ARRAY;
        } else if (type == JavaKind.Short) {
            return OCLAssembler.OCLBinaryTemplate.NEW_PRIVATE_SHORT_ARRAY;
        } else if (type == JavaKind.Long) {
            return OCLAssembler.OCLBinaryTemplate.NEW_PRIVATE_LONG_ARRAY;
        } else if (type == JavaKind.Char) {
            return OCLAssembler.OCLBinaryTemplate.NEW_PRIVATE_CHAR_ARRAY;
        } else if (type == JavaKind.Byte) {
            return OCLAssembler.OCLBinaryTemplate.NEW_PRIVATE_BYTE_ARRAY;
        }
        return null;
    }

    public static OCLAssembler.OCLBinaryTemplate resolveTemplateType(JavaKind type) {
        if (type == JavaKind.Int) {
            return OCLAssembler.OCLBinaryTemplate.NEW_LOCAL_INT_ARRAY;
        } else if (type == JavaKind.Double) {
            return OCLAssembler.OCLBinaryTemplate.NEW_LOCAL_DOUBLE_ARRAY;
        } else if (type == JavaKind.Float) {
            return OCLAssembler.OCLBinaryTemplate.NEW_LOCAL_FLOAT_ARRAY;
        } else if (type == JavaKind.Short) {
            return OCLAssembler.OCLBinaryTemplate.NEW_LOCAL_SHORT_ARRAY;
        } else if (type == JavaKind.Long) {
            return OCLAssembler.OCLBinaryTemplate.NEW_LOCAL_LONG_ARRAY;
        } else if (type == JavaKind.Char) {
            return OCLAssembler.OCLBinaryTemplate.NEW_LOCAL_CHAR_ARRAY;
        } else if (type == JavaKind.Byte) {
            return OCLAssembler.OCLBinaryTemplate.NEW_LOCAL_BYTE_ARRAY;
        }
        return null;
    }

    public static OCLAssembler.OCLBinaryTemplate resolveTemplateType(ResolvedJavaType type) {
        return resolveTemplateType(type.getJavaKind());
    }

    public static OCLAssembler.OCLBinaryTemplate resolvePrivateTemplateType(ResolvedJavaType type) {
        return resolvePrivateTemplateType(type.getJavaKind());
    }

    private final int size;
    private final int vectorLength;

    private final OCLKind kind;
    private final OCLKind elementKind;
    private final Class<?> javaClass;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private final EnumKey key = new EnumKey(this);

    OCLKind(int size, Class<?> javaClass) {
        this(size, javaClass, null);
    }

    OCLKind(int size, Class<?> javaClass, OCLKind kind) {
        this.kind = this;
        this.javaClass = javaClass;
        this.elementKind = kind;
        this.size = (elementKind == null) ? size : elementKind.size * size;
        this.vectorLength = (elementKind == null) ? 1 : size;
    }

    @Override
    public Key getKey() {
        return key;
    }

    @Override
    public int getSizeInBytes() {
        if (vectorLength == 3) {
            return size + elementKind.getSizeInBytes();
        }
        return size;
    }

    public Class<?> getJavaClass() {
        guarantee(javaClass != null, "undefined java class for: %s", this);
        return javaClass;
    }

    @Override
    public int getVectorLength() {
        return vectorLength;
    }

    public OCLKind getElementKind() {
        return (isVector()) ? elementKind : ILLEGAL;
    }

    @Override
    public char getTypeChar() {

        switch (kind) {
            case BOOL:
                return 'z';
            case CHAR:
            case UCHAR:
                return 'c';
            case SHORT:
            case USHORT:
                return 's';
            case INT:
            case UINT:
            case ATOMIC_ADD_INT:
            case ATOMIC_SUB_INT:
            case ATOMIC_MUL_INT:
                return 'i';
            case LONG:
            case ULONG:
            case ATOMIC_ADD_LONG:
                return 'l';
            case HALF:
                return 'h';
            case FLOAT:
            case ATOMIC_ADD_FLOAT:
                return 'f';
            case DOUBLE:
                return 'd';
            case CHAR2:
            case UCHAR2:
            case SHORT2:
            case USHORT2:
            case INT2:
            case UINT2:
            case LONG2:
            case ULONG2:
            case FLOAT2:
            case DOUBLE2:
            case CHAR3:
            case UCHAR3:
            case SHORT3:
            case USHORT3:
            case INT3:
            case UINT3:
            case LONG3:
            case ULONG3:
            case FLOAT3:
            case DOUBLE3:
            case CHAR4:
            case UCHAR4:
            case SHORT4:
            case USHORT4:
            case INT4:
            case UINT4:
            case LONG4:
            case ULONG4:
            case FLOAT4:
            case DOUBLE4:
            case CHAR8:
            case UCHAR8:
            case SHORT8:
            case USHORT8:
            case INT8:
            case UINT8:
            case LONG8:
            case ULONG8:
            case FLOAT8:
            case DOUBLE8:
            case CHAR16:
            case UCHAR16:
            case SHORT16:
            case USHORT16:
            case INT16:
            case UINT16:
            case LONG16:
            case ULONG16:
            case FLOAT16:
            case DOUBLE16:
                return 'v';
            default:
                return '-';
        }
    }

    @Override
    public String toString() {
        if (this == OCLKind.ATOMIC_ADD_INT) {
            return "int";
        } else if (this == OCLKind.ATOMIC_SUB_INT) {
            return "int";
        } else if (this == OCLKind.ATOMIC_MUL_INT) {
            return "int";
        } else if (this == OCLKind.ATOMIC_ADD_LONG) {
            return "long";
        } else if (this == OCLKind.ATOMIC_ADD_FLOAT) {
            return "float";
        } else {
            return name().toLowerCase();
        }
    }

    public String getTypePrefix() {
        StringBuilder sb = new StringBuilder();
        if (isVector()) {
            sb.append('v');
            sb.append(getVectorLength());
        }
        if (isUnsigned()) {
            sb.append('u');
        }

        if (isVector()) {
            sb.append(getElementKind().getTypeChar());
        } else {
            sb.append(getTypeChar());
        }

        return sb.toString();
    }

    public boolean isUnsigned() {
        if (!isInteger()) {
            return false;
        } else {
            return kind.name().charAt(0) == 'U';
        }
    }

    public boolean isInteger() {
        return kind != ILLEGAL && !isFloating();
    }

    public boolean isFloating() {
        // TODO are vectors integers?
        return kind == FLOAT || kind == DOUBLE;
    }

    public boolean isVector() {
        return vectorLength > 1;
    }

    public boolean isPrimitive() {
        return (vectorLength == 1 && kind != OCLKind.ILLEGAL);
    }

    public JavaConstant getDefaultValue() {
        if (!isVector()) {
            return JavaConstant.defaultForKind(asJavaKind());
        }
        unimplemented();
        return JavaConstant.NULL_POINTER;
    }

    public static OCLKind resolveToVectorKind(ResolvedJavaType type) {
        if (!type.isPrimitive() && type.getAnnotation(Vector.class) != null) {
            String typeName = type.getName();
            int index = typeName.lastIndexOf("/");
            String simpleName = typeName.substring(index + 1, typeName.length() - 1).toUpperCase();
            if (simpleName.startsWith("BYTE")) {
                simpleName = simpleName.replace("BYTE", "CHAR");
            }
            return OCLKind.valueOf(simpleName);
        }
        return OCLKind.ILLEGAL;
    }

    public int getByteCount() {
        return size;
    }

    public static int lookupTypeIndex(OCLKind kind) {
        switch (kind) {
            case SHORT:
                return 0;
            case INT:
                return 1;
            case FLOAT:
                return 2;
            case CHAR:
                return 3;
            case DOUBLE:
                return 4;
            default:
                return -1;
        }
    }

    public final int lookupLengthIndex() {
        return lookupLengthIndex(getVectorLength());
    }

    public final int lookupTypeIndex() {
        return lookupTypeIndex(getElementKind());
    }

    public static int lookupLengthIndex(int length) {
        switch (length) {
            case 2:
                return 0;
            case 3:
                return 1;
            case 4:
                return 2;
            case 8:
                return 3;
            case 16:
                return 4;
            default:
                return -1;
        }
    }

    public JavaKind asJavaKind() {
        if (kind != ILLEGAL && !kind.isVector()) {
            switch (kind) {
                case BOOL:
                    return JavaKind.Boolean;
                case CHAR:
                case UCHAR:
                    return JavaKind.Byte;
                case SHORT:
                case USHORT:
                    return JavaKind.Short;
                case INT:
                case UINT:
                    return JavaKind.Int;
                case LONG:
                case ULONG:
                    return JavaKind.Long;
                case FLOAT:
                    return JavaKind.Float;
                case DOUBLE:
                    return JavaKind.Double;
                default:
                    shouldNotReachHere();
            }
        }
        return JavaKind.Illegal;
    }
}
