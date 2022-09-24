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
package uk.ac.manchester.tornado.drivers.opencl.graal;

import org.graalvm.compiler.java.GraphBuilderPhase;
import org.graalvm.compiler.nodes.graphbuilderconf.GraphBuilderConfiguration;
import org.graalvm.compiler.nodes.graphbuilderconf.GraphBuilderConfiguration.Plugins;
import org.graalvm.compiler.nodes.graphbuilderconf.InvocationPlugins;
import org.graalvm.compiler.options.OptionValues;
import org.graalvm.compiler.phases.PhaseSuite;
import org.graalvm.compiler.phases.common.AddressLoweringPhase.AddressLowering;
import org.graalvm.compiler.phases.tiers.HighTierContext;

import jdk.vm.ci.meta.MetaAccessProvider;
import jdk.vm.ci.meta.ResolvedJavaMethod;
import uk.ac.manchester.tornado.api.TornadoDeviceContext;
import uk.ac.manchester.tornado.drivers.opencl.graal.compiler.OCLCanonicalizer;
import uk.ac.manchester.tornado.drivers.opencl.graal.compiler.OCLCompilerConfiguration;
import uk.ac.manchester.tornado.runtime.graal.TornadoLIRSuites;
import uk.ac.manchester.tornado.runtime.graal.TornadoSuites;
import uk.ac.manchester.tornado.runtime.graal.compiler.TornadoSketchTier;
import uk.ac.manchester.tornado.runtime.graal.compiler.TornadoSuitesProvider;
import uk.ac.manchester.tornado.runtime.tasks.meta.TaskMetaData;

public class OCLSuitesProvider implements TornadoSuitesProvider {

    private final PhaseSuite<HighTierContext> graphBuilderSuite;
    private final TornadoSuites suites;
    private final TornadoLIRSuites lirSuites;
    private final OCLCanonicalizer canonicalizer;

    public OCLSuitesProvider(OptionValues options, TornadoDeviceContext deviceContext, Plugins plugins, MetaAccessProvider metaAccessProvider, OCLCompilerConfiguration compilerConfig,
            AddressLowering addressLowering) {
        graphBuilderSuite = createGraphBuilderSuite(plugins);
        canonicalizer = new OCLCanonicalizer();
        suites = new TornadoSuites(options, deviceContext, compilerConfig, metaAccessProvider, canonicalizer, addressLowering);
        lirSuites = new TornadoLIRSuites(suites.getPreAllocationOptimizationStage(), suites.getAllocationStage(), suites.getPostAllocationOptimizationStage());
    }

    private PhaseSuite<HighTierContext> createGraphBuilderSuite(Plugins plugins) {
        PhaseSuite<HighTierContext> suite = new PhaseSuite<>();

        GraphBuilderConfiguration config = GraphBuilderConfiguration.getSnippetDefault(plugins);
        config.withEagerResolving(true);

        // config.setUseProfiling(false);
        suite.appendPhase(new GraphBuilderPhase(config));

        return suite;
    }

    public TornadoSuites getSuites() {
        return suites;
    }

    @Override
    public PhaseSuite<HighTierContext> getGraphBuilderSuite() {
        return graphBuilderSuite;
    }

    public TornadoLIRSuites getLIRSuites() {
        return lirSuites;
    }

    @Override
    public TornadoSketchTier getSketchTier() {
        return suites.getSketchTier();
    }

}
