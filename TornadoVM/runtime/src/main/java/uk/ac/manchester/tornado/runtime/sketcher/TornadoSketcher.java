/*
 * This file is part of Tornado: A heterogeneous programming framework:
 * https://github.com/beehive-lab/tornadovm
 *
 * Copyright (c) 2020, APT Group, Department of Computer Science,
 * School of Engineering, The University of Manchester. All rights reserved.
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
package uk.ac.manchester.tornado.runtime.sketcher;

import jdk.vm.ci.meta.ResolvedJavaMethod;
import org.graalvm.compiler.debug.DebugCloseable;
import org.graalvm.compiler.debug.DebugContext;
import org.graalvm.compiler.debug.DebugDumpScope;
import org.graalvm.compiler.debug.TimerKey;
import org.graalvm.compiler.graph.CachedGraph;
import org.graalvm.compiler.nodes.Invoke;
import org.graalvm.compiler.nodes.CallTargetNode;
import org.graalvm.compiler.nodes.ParameterNode;
import org.graalvm.compiler.nodes.StructuredGraph;
import org.graalvm.compiler.nodes.StructuredGraph.AllowAssumptions;
import org.graalvm.compiler.nodes.StructuredGraph.Builder;
import org.graalvm.compiler.nodes.ValueNode;
import org.graalvm.compiler.phases.OptimisticOptimizations;
import org.graalvm.compiler.phases.PhaseSuite;
import org.graalvm.compiler.phases.common.DeadCodeEliminationPhase;
import org.graalvm.compiler.phases.tiers.HighTierContext;
import org.graalvm.compiler.phases.util.Providers;
import uk.ac.manchester.tornado.api.enums.TornadoVMBackendType;
import uk.ac.manchester.tornado.api.common.Access;
import uk.ac.manchester.tornado.api.exceptions.TornadoBailoutRuntimeException;
import uk.ac.manchester.tornado.api.exceptions.TornadoInternalError;
import uk.ac.manchester.tornado.api.exceptions.TornadoRuntimeException;
import uk.ac.manchester.tornado.runtime.common.Tornado;
import uk.ac.manchester.tornado.runtime.graal.compiler.TornadoCompilerIdentifier;
import uk.ac.manchester.tornado.runtime.graal.compiler.TornadoSketchTier;
import uk.ac.manchester.tornado.runtime.graal.phases.TornadoSketchTierContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.graalvm.compiler.phases.common.DeadCodeEliminationPhase.Optionality.Optional;
import static uk.ac.manchester.tornado.api.exceptions.TornadoInternalError.guarantee;
import static uk.ac.manchester.tornado.runtime.TornadoCoreRuntime.getDebugContext;
import static uk.ac.manchester.tornado.runtime.TornadoCoreRuntime.getOptions;
import static uk.ac.manchester.tornado.runtime.TornadoCoreRuntime.getTornadoExecutor;
import static uk.ac.manchester.tornado.runtime.common.Tornado.fatal;
import static uk.ac.manchester.tornado.runtime.common.Tornado.info;

public class TornadoSketcher {

    private static class TornadoSketcherCacheEntry {

        private final int driverIndex;
        private final int deviceIndex;
        private final Future<Sketch> sketchFuture;

        private TornadoSketcherCacheEntry(int driverIndex, int deviceIndex, Future<Sketch> sketchFuture) {
            this.driverIndex = driverIndex;
            this.deviceIndex = deviceIndex;
            this.sketchFuture = sketchFuture;
        }

        public boolean matchesDriverAndDevice(int driverIndex, int deviceIndex) {
            return this.driverIndex == driverIndex && this.deviceIndex == deviceIndex;
        }

        public Future<Sketch> getSketchFuture() {
            return sketchFuture;
        }
    }

    private static final AtomicInteger sketchId = new AtomicInteger(0);

    private static final Map<ResolvedJavaMethod, List<TornadoSketcherCacheEntry>> cache = new ConcurrentHashMap<>();

    private static final TimerKey Sketcher = DebugContext.timer("Sketcher");

    private static final OptimisticOptimizations optimisticOpts = OptimisticOptimizations.ALL;

    private static HashSet<String> openCLTokens = new HashSet<>();
    static {
        // XXX: To be completed
        openCLTokens.add("kernel");
        openCLTokens.add("__kernel");
        openCLTokens.add("__global");
        openCLTokens.add("global");
        openCLTokens.add("local");
        openCLTokens.add("__local");
        openCLTokens.add("private");
        openCLTokens.add("half");
        openCLTokens.add("dot");
        openCLTokens.add("uniform");
        openCLTokens.add("pipe");
        openCLTokens.add("auto");
        openCLTokens.add("cross");
        openCLTokens.add("distance");
        openCLTokens.add("normalize");
        openCLTokens.add("complex");
    }

    private static boolean cacheContainsSketch(ResolvedJavaMethod method, int driverIndex, int deviceIndex) {
        List<TornadoSketcherCacheEntry> entries = cache.get(method);
        if (entries == null) {
            return false;
        }

        synchronized (entries) {
            for (TornadoSketcherCacheEntry entry : entries) {
                if (entry.matchesDriverAndDevice(driverIndex, deviceIndex)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static Sketch lookup(ResolvedJavaMethod resolvedMethod, int driverIndex, int deviceIndex) {
        Sketch sketch = null;
        guarantee(cache.containsKey(resolvedMethod), "cache miss for: %s", resolvedMethod.getName());
        List<TornadoSketcherCacheEntry> entries = cache.get(resolvedMethod);
        try {
            synchronized (entries) {
                for (TornadoSketcherCacheEntry entry : entries) {
                    if (entry.matchesDriverAndDevice(driverIndex, deviceIndex)) {
                        sketch = entry.getSketchFuture().get();
                        break;
                    }
                }
            }
            guarantee(sketch != null, "No sketch available for %d:%d %s", driverIndex, deviceIndex, resolvedMethod.getName());
        } catch (InterruptedException | ExecutionException e) {
            fatal("Failed to retrieve sketch for %d:%d %s ", driverIndex, deviceIndex, resolvedMethod.getName());
            if (Tornado.DEBUG) {
                e.printStackTrace();
            }
            if (e.getCause() instanceof TornadoRuntimeException) {
                throw (TornadoRuntimeException) e.getCause();
            }
            if (e.getCause() instanceof TornadoBailoutRuntimeException) {
                throw (TornadoBailoutRuntimeException) e.getCause();
            }
            throw new TornadoInternalError(e);
        }
        return sketch;
    }

    private static class TornadoSketcherCallable implements Callable<Sketch> {
        private final SketchRequest request;

        public TornadoSketcherCallable(SketchRequest request) {
            this.request = request;
        }

        @Override
        public Sketch call() throws Exception {
            // if (request.resolvedMethod.getName().contains("List")) return null;
            try (DebugContext.Scope ignored = getDebugContext().scope("SketchCompiler")) {
                return buildSketch(request.resolvedMethod, request.providers, request.graphBuilderSuite, request.sketchTier, request.driverIndex, request.deviceIndex);
            } catch (Throwable e) {
                throw getDebugContext().handle(e);
            }
        }
    }

    static void buildSketch(SketchRequest request) {
        if (cacheContainsSketch(request.resolvedMethod, request.driverIndex, request.deviceIndex)) {
            return;
        }
        List<TornadoSketcherCacheEntry> sketches = cache.computeIfAbsent(request.resolvedMethod, k -> Collections.synchronizedList(new ArrayList<>(TornadoVMBackendType.values().length)));
        Future<Sketch> result = getTornadoExecutor().submit(new TornadoSketcherCallable(request));
        sketches.add(new TornadoSketcherCacheEntry(request.driverIndex, request.deviceIndex, result));
    }

    private static Sketch buildSketch(ResolvedJavaMethod resolvedMethod, Providers providers, PhaseSuite<HighTierContext> graphBuilderSuite, TornadoSketchTier sketchTier, int driverIndex,
            int deviceIndex) {

        info("Building sketch of %s", resolvedMethod.getName());
        TornadoCompilerIdentifier id = new TornadoCompilerIdentifier("sketch-" + resolvedMethod.getName(), sketchId.getAndIncrement());
        Builder builder = new Builder(getOptions(), getDebugContext(), AllowAssumptions.YES);
        builder.method(resolvedMethod);
        builder.compilationId(id);
        builder.name("sketch-" + resolvedMethod.getName());
        final StructuredGraph graph = builder.build();

        // Check legal Kernel Name
        if (openCLTokens.contains(resolvedMethod.getName())) {
            throw new TornadoRuntimeException("[ERROR] Java method name corresponds to an OpenCL Token. Change the Java method's name: " + resolvedMethod.getName());
        }

        try (DebugContext.Scope ignored = getDebugContext().scope("Tornado-Sketcher", new DebugDumpScope("Tornado-Sketcher")); DebugCloseable ignored1 = Sketcher.start(getDebugContext())) {
            final TornadoSketchTierContext highTierContext = new TornadoSketchTierContext(providers, graphBuilderSuite, optimisticOpts, resolvedMethod);
            if (graph.start().next() == null) {
                graphBuilderSuite.apply(graph, highTierContext);
                new DeadCodeEliminationPhase(Optional).apply(graph);
            } else {
                getDebugContext().dump(DebugContext.BASIC_LEVEL, graph, "initial state");
            }

            sketchTier.apply(graph, highTierContext);
            graph.maybeCompress();

            // Compile all non-inlined call-targets into a single compilation-unit

            for (Invoke invoke : graph.getInvokes()) {
                // do not sketch functions related to Lists and Collections

                if (invoke.callTarget().targetMethod().toString().contains("List") || invoke.callTarget().targetMethod().toString().contains("Collection")) {
                    continue;
                }

                if (openCLTokens.contains(invoke.callTarget().targetMethod().getName())) {
                    throw new TornadoRuntimeException("[ERROR] Java method name corresponds to an OpenCL Token. Change the Java method's name: " + invoke.callTarget().targetMethod().getName());
                }
                SketchRequest newRequest = new SketchRequest(invoke.callTarget().targetMethod(), providers, graphBuilderSuite, sketchTier, driverIndex, deviceIndex);
                buildSketch(newRequest);
            }

            Access[] methodAccesses = highTierContext.getAccesses();
            for (Invoke invoke : graph.getInvokes()) {
                if (invoke.callTarget().targetMethod().toString().contains("List") || invoke.callTarget().targetMethod().toString().contains("Collection")) {
                    continue;
                }
                Sketch sketch = lookup(invoke.callTarget().targetMethod(), driverIndex, deviceIndex);
                mergeAccesses(methodAccesses, invoke.callTarget(), sketch.getArgumentsAccess());
            }

            return new Sketch(CachedGraph.fromReadonlyCopy(graph), methodAccesses);

        } catch (Throwable e) {
            fatal("unable to build sketch for method: %s (%s)", resolvedMethod.getName(), e.getMessage());
            if (Tornado.DEBUG) {
                e.printStackTrace();
            }
            throw new TornadoBailoutRuntimeException("Unable to build sketch for method: " + resolvedMethod.getName() + "(" + e.getMessage() + ")");
        }
    }

    /**
     * Merges the {@param calleeAccesses} into the {@param callerAccesses}. For
     * example, given the two {@link Access} arrays below, a merge will look like:
     *
     * Caller accesses: NONE, READ, WRITE, NONE, READ_WRITE Callee accesses: READ,
     * WRITE, NONE, READ_WRITE, NONE
     *
     * Updated caller accesses: READ, READ_WRITE, WRITE, READ_WRITE, READ_WRITE
     *
     * This is needed since caller parameters can have different accesses in a
     * callee.
     */
    private static void mergeAccesses(Access[] callerAccesses, CallTargetNode callTarget, Access[] calleeAccesses) {
        List<ValueNode> callArgs = callTarget.arguments().snapshot();

        int index = callTarget.targetMethod().isStatic() ? 0 : 1;

        for (; index < callArgs.size(); index++) {
            ValueNode callArg = callArgs.get(index);
            if (!(callArg instanceof ParameterNode)) {
                continue;
            }
            ParameterNode param = (ParameterNode) callArg;
            int paramIndex = param.index();

            Access calleeAcc = calleeAccesses[index];
            Access callerAcc = callerAccesses[paramIndex];

            callerAccesses[paramIndex] = Access.values()[callerAcc.position | calleeAcc.position];
        }
    }
}
