/*
 * Copyright (c) 2020, APT Group, Department of Computer Science,
 * School of Engineering, The University of Manchester. All rights reserved.
 * Copyright (c) 2013-2020, APT Group, Department of Computer Science,
 * The University of Manchester.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package uk.ac.manchester.tornado.matrix;

import org.ejml.data.SingularMatrixException;
import org.ejml.simple.SimpleMatrix;

import uk.ac.manchester.tornado.api.collections.types.Matrix4x4Float;

public class MatrixFloatOps {

    public static void inverse(Matrix4x4Float m) {
        try {
            SimpleMatrix sm = EjmlUtil.toMatrix(m).invert();
            m.set(EjmlUtil.toMatrix4x4Float(sm));
        } catch (SingularMatrixException e) {
        }
    }
}
