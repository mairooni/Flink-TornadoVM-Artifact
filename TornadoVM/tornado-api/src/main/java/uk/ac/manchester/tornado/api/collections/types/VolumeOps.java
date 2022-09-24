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
package uk.ac.manchester.tornado.api.collections.types;

public class VolumeOps {

    public static Float3 grad(final VolumeShort2 volume, final Float3 dim, final Float3 point) {

        final Float3 scaledPos = new Float3(((point.getX() * volume.X()) / dim.getX()) - 0.5f, ((point.getY() * volume.Y()) / dim.getY()) - 0.5f, ((point.getZ() * volume.Z()) / dim.getZ()) - 0.5f);

        final Float3 tmp = Float3.floor(scaledPos);

        final Float3 factor = Float3.fract(scaledPos);

        final Int3 base = new Int3((int) tmp.getX(), (int) tmp.getY(), (int) tmp.getZ());

        // factor.frac();
        final Int3 zeros = new Int3();
        final Int3 limits = Int3.sub(new Int3(volume.X(), volume.Y(), volume.Z()), 1);

        final Int3 lowerLower = Int3.max(zeros, Int3.sub(base, 1));
        final Int3 lowerUpper = Int3.max(zeros, base);
        final Int3 upperLower = Int3.min(limits, Int3.add(base, 1));
        final Int3 upperUpper = Int3.min(limits, Int3.add(base, 2));

        final Int3 lower = lowerUpper;
        final Int3 upper = upperLower;

        // @formatter:off
        final float gx = ((((((vs(volume, upperLower.getX(), lower.getY(), lower.getZ())) - vs(volume, lowerLower.getX(), lower.getY(), lower.getZ())) * (1 - factor.getX())
                + ((vs(volume, upperUpper.getX(), lower.getY(), lower.getZ())) - vs(volume, lowerUpper.getX(), lower.getY(), lower.getZ())) * factor.getX())
                * (1 - factor.getY()))
                + ((((vs(volume, upperLower.getX(), upper.getY(), lower.getZ())) - vs(volume, lowerLower.getX(), upper.getY(), lower.getZ())) * (1 - factor.getX())
                + ((vs(volume, upperUpper.getX(), upper.getY(), lower.getZ())) - vs(volume, lowerUpper.getX(), upper.getY(), lower.getZ())) * factor.getX())
                * factor.getY())) * (1 - factor.getZ()))
                + ((((((vs(volume, upperLower.getX(), lower.getY(), upper.getZ())
                - vs(volume, lowerLower.getX(), lower.getY(), upper.getZ()))
                * (1 - factor.getX()))
                + ((vs(volume, upperUpper.getX(), lower.getY(), upper.getZ())
                - vs(volume, lowerUpper.getX(), lower.getY(), upper.getZ()))
                * factor.getX()))
                * (1 - factor.getY()))
                + ((((vs(volume, upperLower.getX(), upper.getY(), upper.getZ())
                - vs(volume, lowerLower.getX(), upper.getY(), upper.getZ()))
                * (1 - factor.getX()))
                + ((vs(volume, upperUpper.getX(), upper.getY(), upper.getZ())
                - vs(volume, lowerUpper.getX(), upper.getY(), upper.getZ()))
                * factor.getX()))
                * factor.getY()))
                * factor.getZ());

        final float gy = ((((((vs(volume, lower.getX(), upperLower.getY(), lower.getZ())
                - vs(volume, lower.getX(), lowerLower.getY(), lower.getZ()))
                * (1 - factor.getX()))
                + ((vs(volume, upper.getX(), upperLower.getY(), lower.getZ())
                - vs(volume, upper.getX(), lowerLower.getY(), lower.getZ()))
                * factor.getX()))
                * (1 - factor.getY()))
                + ((((vs(volume, lower.getX(), upperUpper.getY(), lower.getZ())
                - vs(volume, lower.getX(), lowerUpper.getY(), lower.getZ()))
                * (1 - factor.getX()))
                + ((vs(volume, upper.getX(), upperUpper.getY(), lower.getZ())
                - vs(volume, upper.getX(), lowerUpper.getY(), lower.getZ()))
                * factor.getX()))
                * factor.getY()))
                * (1 - factor.getZ()))
                + ((((((vs(volume, lower.getX(), upperLower.getY(), upper.getZ())
                - vs(volume, lower.getX(), lowerLower.getY(), upper.getZ()))
                * (1 - factor.getX()))
                + ((vs(volume, upper.getX(), upperLower.getY(), upper.getZ())
                - vs(volume, upper.getX(), lowerLower.getY(), upper.getZ()))
                * factor.getX()))
                * (1 - factor.getY()))
                + ((((vs(volume, lower.getX(), upperUpper.getY(), upper.getZ())
                - vs(volume, lower.getX(), lowerUpper.getY(), upper.getZ()))
                * (1 - factor.getX()))
                + ((vs(volume, upper.getX(), upperUpper.getY(), upper.getZ())
                - vs(volume, upper.getX(), lowerUpper.getY(), upper.getZ()))
                * factor.getX()))
                * factor.getY()))
                * factor.getZ());

        final float gz = ((((((vs(volume, lower.getX(), lower.getY(), upperLower.getZ()))
                - vs(volume, lower.getX(), lower.getY(), lowerLower.getZ()))
                * (1 - factor.getX()))
                + ((vs(volume, upper.getX(), lower.getY(), upperLower.getZ())
                - vs(volume, upper.getX(), lower.getY(), lowerLower.getZ()))
                * factor.getX())) * (1 - factor.getY()))
                + ((((vs(volume, lower.getX(), upper.getY(), upperLower.getZ())
                - vs(volume, lower.getX(), upper.getY(), lowerLower.getZ()))
                * (1 - factor.getX()))
                + ((vs(volume, upper.getX(), upper.getY(), upperLower.getZ())
                - vs(volume, upper.getX(), upper.getY(), lowerLower.getZ())) * factor
                .getX())) * factor.getY())) * (1 - factor.getZ())
                + ((((((vs(volume, lower.getX(), lower.getY(), upperUpper.getZ())
                - vs(volume, lower.getX(), lower.getY(), lowerUpper.getZ()))
                * (1 - factor.getX()))
                + ((vs(volume, upper.getX(), lower.getY(), upperUpper.getZ())
                - vs(volume, upper.getX(), lower.getY(), lowerUpper.getZ()))
                * factor.getX()))
                * (1 - factor.getY()))
                + ((((vs(volume, lower.getX(), upper.getY(), upperUpper.getZ())
                - vs(volume, lower.getX(), upper.getY(), lowerUpper.getZ()))
                * (1 - factor.getX()))
                + ((vs(volume, upper.getX(), upper.getY(), upperUpper.getZ())
                - vs(volume, upper.getX(), upper.getY(), lowerUpper.getZ()))
                * factor.getX()))
                * factor.getY())) * factor.getZ());
        // @formatter:on
        final Float3 tmp1 = Float3.mult(new Float3(dim.getX() / volume.X(), dim.getY() / volume.Y(), dim.getZ() / volume.Z()), (0.5f * 0.00003051944088f));

        return Float3.mult(new Float3(gx, gy, gz), tmp1);

    }

    public static float interp(final VolumeShort2 volume, final Float3 dim, final Float3 point) {

        final Float3 scaledPos = new Float3((point.getX() * volume.X() / dim.getX()) - 0.5f, (point.getY() * volume.Y() / dim.getY()) - 0.5f, (point.getZ() * volume.Z() / dim.getZ()) - 0.5f);

        final Float3 tmp = Float3.floor(scaledPos);
        final Float3 factor = Float3.fract(scaledPos);

        final Int3 base = new Int3((int) tmp.getX(), (int) tmp.getY(), (int) tmp.getZ());

        final Int3 zeros = new Int3(0, 0, 0);
        final Int3 limits = Int3.sub(new Int3(volume.X(), volume.Y(), volume.Z()), 1);

        final Int3 lower = Int3.max(base, zeros);
        final Int3 upper = Int3.min(limits, Int3.add(base, 1));

        final float factorX = (1 - factor.getX());
        final float factorY = (1 - factor.getY());
        final float factorZ = (1 - factor.getZ());

        final float c00 = (vs(volume, lower.getX(), lower.getY(), lower.getZ()) * factorX) + (vs(volume, upper.getX(), lower.getY(), lower.getZ()) * factor.getX());
        final float c10 = (vs(volume, lower.getX(), upper.getY(), lower.getZ()) * factorX) + (vs(volume, upper.getX(), upper.getY(), lower.getZ()) * factor.getX());

        final float c01 = (vs(volume, lower.getX(), lower.getY(), upper.getZ()) * factorX) + (vs(volume, upper.getX(), lower.getY(), upper.getZ()) * factor.getX());

        final float c11 = (vs(volume, lower.getX(), upper.getY(), upper.getZ()) * factorX) + (vs(volume, upper.getX(), upper.getY(), upper.getZ()) * factor.getX());

        final float c0 = (c00 * factorY) + (c10 * factor.getY());
        final float c1 = (c01 * factorY) + (c11 * factor.getY());

        final float c = (c0 * factorZ) + (c1 * factor.getZ());

        return c * 0.00003051944088f;
    }

    public static float vs1(int x, int y, int z, VolumeShort2 v) {
        return vs(v, x, y, z);
    }

    public static float vs(final VolumeShort2 cube, int x, int y, int z) {
        return cube.get(x, y, z).getX();
    }

}
