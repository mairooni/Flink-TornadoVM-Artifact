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
package uk.ac.manchester.tornado.drivers.opencl.graal.compiler;

import static org.graalvm.compiler.phases.common.DeadCodeEliminationPhase.Optionality.Optional;
import static uk.ac.manchester.tornado.api.exceptions.TornadoInternalError.guarantee;
import static uk.ac.manchester.tornado.runtime.TornadoCoreRuntime.getDebugContext;
import static uk.ac.manchester.tornado.runtime.common.Tornado.DUMP_COMPILED_METHODS;
import static uk.ac.manchester.tornado.runtime.common.Tornado.error;
import static uk.ac.manchester.tornado.runtime.common.Tornado.info;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.graalvm.compiler.code.CompilationResult;
import org.graalvm.compiler.core.common.alloc.ComputeBlockOrder;
import org.graalvm.compiler.core.common.alloc.RegisterAllocationConfig;
import org.graalvm.compiler.core.common.cfg.AbstractBlockBase;
import org.graalvm.compiler.debug.DebugCloseable;
import org.graalvm.compiler.debug.DebugContext;
import org.graalvm.compiler.debug.DebugDumpScope;
import org.graalvm.compiler.debug.TimerKey;
import org.graalvm.compiler.lir.LIR;
import org.graalvm.compiler.lir.asm.CompilationResultBuilderFactory;
import org.graalvm.compiler.lir.framemap.FrameMap;
import org.graalvm.compiler.lir.framemap.FrameMapBuilder;
import org.graalvm.compiler.lir.gen.LIRGenerationResult;
import org.graalvm.compiler.lir.gen.LIRGeneratorTool;
import org.graalvm.compiler.lir.phases.AllocationPhase.AllocationContext;
import org.graalvm.compiler.lir.phases.PreAllocationOptimizationPhase.PreAllocationOptimizationContext;
import org.graalvm.compiler.nodes.StructuredGraph;
import org.graalvm.compiler.nodes.StructuredGraph.AllowAssumptions;
import org.graalvm.compiler.nodes.StructuredGraph.Builder;
import org.graalvm.compiler.nodes.StructuredGraph.ScheduleResult;
import org.graalvm.compiler.nodes.cfg.Block;
import org.graalvm.compiler.nodes.spi.NodeLIRBuilderTool;
import org.graalvm.compiler.options.OptionValues;
import org.graalvm.compiler.phases.OptimisticOptimizations;
import org.graalvm.compiler.phases.PhaseSuite;
import org.graalvm.compiler.phases.common.DeadCodeEliminationPhase;
import org.graalvm.compiler.phases.tiers.HighTierContext;
import org.graalvm.compiler.phases.util.Providers;

import jdk.vm.ci.code.RegisterConfig;
import jdk.vm.ci.meta.Assumptions;
import jdk.vm.ci.meta.DefaultProfilingInfo;
import jdk.vm.ci.meta.ProfilingInfo;
import jdk.vm.ci.meta.ResolvedJavaMethod;
import jdk.vm.ci.meta.TriState;
import uk.ac.manchester.tornado.api.exceptions.TornadoInternalError;
import uk.ac.manchester.tornado.drivers.opencl.OCLDeviceContext;
import uk.ac.manchester.tornado.drivers.opencl.OCLTargetDescription;
import uk.ac.manchester.tornado.drivers.opencl.graal.OCLProviders;
import uk.ac.manchester.tornado.drivers.opencl.graal.OCLSuitesProvider;
import uk.ac.manchester.tornado.drivers.opencl.graal.backend.OCLBackend;
import uk.ac.manchester.tornado.drivers.opencl.graal.compiler.OCLLIRGenerationPhase.LIRGenerationContext;
import uk.ac.manchester.tornado.runtime.TornadoCoreRuntime;
import uk.ac.manchester.tornado.runtime.common.Tornado;
import uk.ac.manchester.tornado.runtime.graal.TornadoLIRSuites;
import uk.ac.manchester.tornado.runtime.graal.TornadoSuites;
import uk.ac.manchester.tornado.runtime.graal.compiler.TornadoCompilerIdentifier;
import uk.ac.manchester.tornado.runtime.graal.phases.TornadoHighTierContext;
import uk.ac.manchester.tornado.runtime.graal.phases.TornadoLowTierContext;
import uk.ac.manchester.tornado.runtime.graal.phases.TornadoMidTierContext;
import uk.ac.manchester.tornado.runtime.sketcher.Sketch;
import uk.ac.manchester.tornado.runtime.sketcher.TornadoSketcher;
import uk.ac.manchester.tornado.runtime.tasks.CompilableTask;
import uk.ac.manchester.tornado.runtime.tasks.meta.TaskMetaData;

/**
 * Static methods for orchestrating the compilation of a
 * {@linkplain StructuredGraph graph}.
 */
public class OCLCompiler {

    private static final AtomicInteger compilationId = new AtomicInteger();

    private static final TimerKey CompilerTimer = DebugContext.timer("OpenCLGraalCompiler");
    private static final TimerKey FrontEnd = DebugContext.timer("OpenCLFrontend");
    private static final TimerKey BackEnd = DebugContext.timer("OpenCLBackend");
    private static final TimerKey EmitLIR = DebugContext.timer("OpenCLEmitLIR");
    private static final TimerKey EmitCode = DebugContext.timer("OpenCLEmitCode");

    private static final OCLLIRGenerationPhase LIR_GENERATION_PHASE = new OCLLIRGenerationPhase();

    // FIXME <REFACTOR> Remove the inheritance (See SPIRV and PTX)
    public static class Request<T extends OCLCompilationResult> {

        public final StructuredGraph graph;
        public final ResolvedJavaMethod installedCodeOwner;
        public final Object[] args;
        public final TaskMetaData meta;
        public final Providers providers;
        public final OCLBackend backend;
        public final PhaseSuite<HighTierContext> graphBuilderSuite;
        public final OptimisticOptimizations optimisticOpts;
        public final ProfilingInfo profilingInfo;
        public final TornadoSuites suites;
        public final TornadoLIRSuites lirSuites;
        public final T compilationResult;
        public final CompilationResultBuilderFactory factory;
        public final boolean isKernel;
        public final boolean buildGraph;
        public final long batchThreads;

        public Request(StructuredGraph graph, ResolvedJavaMethod installedCodeOwner, Object[] args, TaskMetaData meta, Providers providers, OCLBackend backend,
                PhaseSuite<HighTierContext> graphBuilderSuite, OptimisticOptimizations optimisticOpts, ProfilingInfo profilingInfo, TornadoSuites suites, TornadoLIRSuites lirSuites,
                T compilationResult, CompilationResultBuilderFactory factory, boolean isKernel, boolean buildGraph, long batchThreads) {
            this.graph = graph;
            this.installedCodeOwner = installedCodeOwner;
            this.args = args;
            this.meta = meta;
            this.providers = providers;
            this.backend = backend;
            this.graphBuilderSuite = graphBuilderSuite;
            this.optimisticOpts = optimisticOpts;
            this.profilingInfo = profilingInfo;
            this.suites = suites;
            this.lirSuites = lirSuites;
            this.compilationResult = compilationResult;
            this.factory = factory;
            this.isKernel = isKernel;
            this.buildGraph = buildGraph;
            this.batchThreads = batchThreads;
        }

        /**
         * Executes this compilation request.
         *
         * @return the result of the compilation
         */
        public T execute() {
            return OCLCompiler.compile(this);
        }
    }

    /**
     * Services a given compilation request.
     *
     * @return the result of the compilation
     */
    public static <T extends OCLCompilationResult> T compile(Request<T> r) {
        assert !r.graph.isFrozen();
        try (DebugContext.Scope s0 = getDebugContext().scope("GraalCompiler", r.graph, r.providers.getCodeCache()); DebugCloseable a = CompilerTimer.start(getDebugContext())) {
            emitFrontEnd(r.providers, r.backend, r.installedCodeOwner, r.args, r.meta, r.graph, r.graphBuilderSuite, r.optimisticOpts, r.profilingInfo, r.suites, r.isKernel, r.buildGraph,
                    r.batchThreads);
            boolean isParallel = false;
            /**
             * A task is determined as parallel if: (i) it has loops annotated with
             * {@link uk.ac.manchester.tornado.api.annotations.Parallel} which corresponds
             * to use a domain with depth greater than zero, or (ii) it uses the
             * GridScheduler.
             */
            if (r.meta != null && (r.meta.isParallel() || (r.meta.isGridSchedulerEnabled() && !r.meta.isGridSequential()))) {
                isParallel = true;
            }
            emitBackEnd(r.graph, null, r.installedCodeOwner, r.backend, r.compilationResult, r.factory, null, r.lirSuites, r.isKernel, isParallel);
        } catch (Throwable e) {
            throw getDebugContext().handle(e);
        }
        return r.compilationResult;
    }

    public static ProfilingInfo getProfilingInfo(StructuredGraph graph) {
        if (graph.method() != null) {
            return graph.method().getProfilingInfo();
        } else {
            return DefaultProfilingInfo.get(TriState.UNKNOWN);
        }
    }

    private static boolean isGraphEmpty(StructuredGraph graph) {
        return graph.start().next() == null;
    }

    /**
     * Builds the graph, optimizes it.
     */
    private static void emitFrontEnd(Providers providers, OCLBackend backend, ResolvedJavaMethod method, Object[] args, TaskMetaData meta, StructuredGraph graph,
            PhaseSuite<HighTierContext> graphBuilderSuite, OptimisticOptimizations optimisticOpts, ProfilingInfo profilingInfo, TornadoSuites suites, boolean isKernel, boolean buildGraph,
            long batchThreads) {
        try (DebugContext.Scope s = getDebugContext().scope("OpenCLFrontend", new DebugDumpScope("OpenCLFrontend")); DebugCloseable a = FrontEnd.start(getDebugContext())) {

            /*
             * Register metadata with all tornado phases
             */
            ((OCLCanonicalizer) suites.getHighTier().getCustomCanonicalizer()).setContext(providers.getMetaAccess(), method, args, meta);

            final TornadoHighTierContext highTierContext = new TornadoHighTierContext(providers, graphBuilderSuite, optimisticOpts, method, args, meta, isKernel, batchThreads);

            if (buildGraph) {
                if (isGraphEmpty(graph)) {
                    graphBuilderSuite.apply(graph, highTierContext);
                    new DeadCodeEliminationPhase(Optional).apply(graph);
                } else {
                    getDebugContext().dump(DebugContext.INFO_LEVEL, graph, "initial state");
                }
            }
            suites.getHighTier().apply(graph, highTierContext);
            graph.maybeCompress();

            final TornadoMidTierContext midTierContext = new TornadoMidTierContext(providers, backend, optimisticOpts, profilingInfo, method, args, meta);
            suites.getMidTier().apply(graph, midTierContext);

            graph.maybeCompress();

            final TornadoLowTierContext lowTierContext = new TornadoLowTierContext(providers, backend, meta);
            suites.getLowTier().apply(graph, lowTierContext);

            getDebugContext().dump(DebugContext.BASIC_LEVEL, graph.getLastSchedule(), "Final HIR schedule");
        } catch (Throwable e) {
            throw getDebugContext().handle(e);
        }
    }

    private static <T extends OCLCompilationResult> void emitBackEnd(StructuredGraph graph, Object stub, ResolvedJavaMethod installedCodeOwner, OCLBackend backend, T compilationResult,
            CompilationResultBuilderFactory factory, RegisterConfig registerConfig, TornadoLIRSuites lirSuites, boolean isKernel, boolean isParallel) {
        try (DebugContext.Scope s = getDebugContext().scope("OpenCLBackend", graph.getLastSchedule()); DebugCloseable a = BackEnd.start(getDebugContext())) {
            LIRGenerationResult lirGen = null;
            lirGen = emitLIR(backend, graph, stub, registerConfig, lirSuites, compilationResult, isKernel);
            try (DebugContext.Scope s2 = getDebugContext().scope("OpenCLCodeGen", lirGen, lirGen.getLIR())) {
                compilationResult.setHasUnsafeAccess(graph.hasUnsafeAccess());
                emitCode(backend, graph.getAssumptions(), graph.method(), graph.getMethods(), lirGen, compilationResult, installedCodeOwner, factory, isKernel, isParallel);
            } catch (Throwable e) {
                throw getDebugContext().handle(e);
            }
        } catch (Throwable e) {
            throw getDebugContext().handle(e);
        }
    }

    public static <T extends CompilationResult> LIRGenerationResult emitLIR(OCLBackend backend, StructuredGraph graph, Object stub, RegisterConfig registerConfig, TornadoLIRSuites lirSuites,
            T compilationResult, boolean isKernel) {
        try {
            return emitLIR0(backend, graph, stub, registerConfig, lirSuites, compilationResult, isKernel);
        } catch (Throwable e) {
            throw new TornadoInternalError(e);
        }
    }

    protected static <T extends CompilationResult> String getCompilationUnitName(StructuredGraph graph, T compilationResult) {
        if (compilationResult != null && compilationResult.getName() != null) {
            return compilationResult.getName();
        }
        ResolvedJavaMethod method = graph.method();
        if (method == null) {
            return "<unknown>";
        }
        return method.format("%H.%n(%p)");
    }

    private static <T extends CompilationResult> LIRGenerationResult emitLIR0(OCLBackend backend, StructuredGraph graph, Object stub, RegisterConfig registerConfig, TornadoLIRSuites lirSuites,
            T compilationResult, boolean isKernel) {
        try (DebugContext.Scope ds = getDebugContext().scope("EmitLIR"); DebugCloseable a = EmitLIR.start(getDebugContext())) {
            OptionValues options = graph.getOptions();
            ScheduleResult schedule = graph.getLastSchedule();
            Block[] blocks = schedule.getCFG().getBlocks();
            Block startBlock = schedule.getCFG().getStartBlock();
            assert startBlock != null;
            assert startBlock.getPredecessorCount() == 0;

            LIR lir = null;
            AbstractBlockBase<?>[] codeEmittingOrder = null;
            AbstractBlockBase<?>[] linearScanOrder = null;
            try (DebugContext.Scope s = getDebugContext().scope("ComputeLinearScanOrder", lir)) {
                codeEmittingOrder = ComputeBlockOrder.computeCodeEmittingOrder(blocks.length, startBlock);
                linearScanOrder = ComputeBlockOrder.computeLinearScanOrder(blocks.length, startBlock);

                lir = new LIR(schedule.getCFG(), linearScanOrder, codeEmittingOrder, options, getDebugContext());
                getDebugContext().dump(DebugContext.INFO_LEVEL, lir, "After linear scan order");
            } catch (Throwable e) {
                throw getDebugContext().handle(e);
            }
            RegisterAllocationConfig registerAllocationConfig = backend.newRegisterAllocationConfig(registerConfig, new String[] {});
            FrameMapBuilder frameMapBuilder = backend.newFrameMapBuilder(registerConfig);
            LIRGenerationResult lirGenRes = backend.newLIRGenerationResult(graph.compilationId(), lir, frameMapBuilder, registerAllocationConfig, graph, stub);
            LIRGeneratorTool lirGen = backend.newLIRGenerator(lirGenRes);
            NodeLIRBuilderTool nodeLirGen = backend.newNodeLIRBuilder(graph, lirGen);

            // LIR generation
            LIRGenerationContext context = new LIRGenerationContext(lirGen, nodeLirGen, graph, schedule, isKernel);
            LIR_GENERATION_PHASE.apply(backend.getTarget(), lirGenRes, context);

            try (DebugContext.Scope s = getDebugContext().scope("LIRStages", nodeLirGen, lir)) {
                getDebugContext().dump(DebugContext.BASIC_LEVEL, lir, "After LIR generation");
                LIRGenerationResult result = emitLowLevel(backend.getTarget(), lirGenRes, lirGen, lirSuites, registerAllocationConfig);
                getDebugContext().dump(DebugContext.BASIC_LEVEL, lir, "Before code generation");
                return result;
            } catch (Throwable e) {
                throw getDebugContext().handle(e);
            }
        } catch (Throwable e) {
            throw getDebugContext().handle(e);
        }
    }

    public static LIRGenerationResult emitLowLevel(OCLTargetDescription target, LIRGenerationResult lirGenRes, LIRGeneratorTool lirGen, TornadoLIRSuites lirSuites,
            RegisterAllocationConfig registerAllocationConfig) {
        final PreAllocationOptimizationContext preAllocOptContext = new PreAllocationOptimizationContext(lirGen);
        lirSuites.getPreAllocationStage().apply(target, lirGenRes, preAllocOptContext);
        AllocationContext allocContext = new AllocationContext(lirGen.getSpillMoveFactory(), registerAllocationConfig);
        lirSuites.getAllocationStage().apply(target, lirGenRes, allocContext);
        return lirGenRes;
    }

    public static void emitCode(OCLBackend backend, Assumptions assumptions, ResolvedJavaMethod rootMethod, List<ResolvedJavaMethod> inlinedMethods, LIRGenerationResult lirGenRes,
            OCLCompilationResult compilationResult, ResolvedJavaMethod installedCodeOwner, CompilationResultBuilderFactory factory, boolean isKernel, boolean isParallel) {
        try (DebugCloseable a = EmitCode.start(getDebugContext())) {
            FrameMap frameMap = lirGenRes.getFrameMap();
            final OCLCompilationResultBuilder crb = backend.newCompilationResultBuilder(frameMap, compilationResult, factory, isKernel, isParallel);
            backend.emitCode(crb, lirGenRes.getLIR(), installedCodeOwner);

            if (assumptions != null && !assumptions.isEmpty()) {
                compilationResult.setAssumptions(assumptions.toArray());
            }
            if (inlinedMethods != null) {
                compilationResult.setMethods(rootMethod, inlinedMethods);
            }

            compilationResult.setNonInlinedMethods(crb.getNonInlinedMethods());
            crb.finish();

            if (getDebugContext().isCountEnabled()) {
                DebugContext.counter("CompilationResults").increment(getDebugContext());
                DebugContext.counter("CodeBytesEmitted").add(getDebugContext(), compilationResult.getTargetCodeSize());
            }

            getDebugContext().dump(DebugContext.BASIC_LEVEL, compilationResult, "After code generation");
        }
    }

    public static OCLCompilationResult compileCodeForDevice(ResolvedJavaMethod resolvedMethod, Object[] args, TaskMetaData meta, OCLProviders providers, OCLBackend backend) {
        return compileCodeForDevice(resolvedMethod, args, meta, providers, backend, 0);

    }

    public static OCLCompilationResult compileCodeForDevice(ResolvedJavaMethod resolvedMethod, Object[] args, TaskMetaData meta, OCLProviders providers, OCLBackend backend, long batchThreads) {
        Tornado.info("Compiling %s on %s", resolvedMethod.getName(), backend.getDeviceContext().getDevice().getDeviceName());
        final TornadoCompilerIdentifier id = new TornadoCompilerIdentifier("compile-kernel" + resolvedMethod.getName(), compilationId.getAndIncrement());

        Builder builder = new Builder(TornadoCoreRuntime.getOptions(), getDebugContext(), AllowAssumptions.YES);
        builder.method(resolvedMethod);
        builder.compilationId(id);
        builder.name("compile-kernel" + resolvedMethod.getName());

        final StructuredGraph kernelGraph = builder.build();

        OptimisticOptimizations optimisticOpts = OptimisticOptimizations.ALL;
        ProfilingInfo profilingInfo = resolvedMethod.getProfilingInfo();

        OCLCompilationResult kernelCompResult = new OCLCompilationResult("internal", resolvedMethod.getName(), meta, backend);
        CompilationResultBuilderFactory factory = CompilationResultBuilderFactory.Default;

        final OCLSuitesProvider suitesProvider = providers.getSuitesProvider();
        Request<OCLCompilationResult> kernelCompilationRequest = new Request<>(kernelGraph, resolvedMethod, args, meta, providers, backend, suitesProvider.getGraphBuilderSuite(), optimisticOpts,
                profilingInfo, suitesProvider.getSuites(), suitesProvider.getLIRSuites(), kernelCompResult, factory, true, true, batchThreads);

        kernelCompilationRequest.execute();

        final Deque<ResolvedJavaMethod> workList = new ArrayDeque<>(kernelCompResult.getNonInlinedMethods());

        while (!workList.isEmpty()) {
            Builder builder1 = new Builder(TornadoCoreRuntime.getOptions(), getDebugContext(), AllowAssumptions.YES);
            builder1.method(resolvedMethod);
            builder1.compilationId(id);
            final ResolvedJavaMethod currentMethod = workList.pop();
            builder1.name("internal" + currentMethod.getName());
            final StructuredGraph graph = builder.build();
            final OCLCompilationResult compResult = new OCLCompilationResult("internal", currentMethod.getName(), meta, backend);
            Request<OCLCompilationResult> methodCompilationRequest = new Request<>(graph, currentMethod, null, null, providers, backend, suitesProvider.getGraphBuilderSuite(), optimisticOpts,
                    profilingInfo, suitesProvider.getSuites(), suitesProvider.getLIRSuites(), compResult, factory, false, true, 0);

            methodCompilationRequest.execute();
            workList.addAll(compResult.getNonInlinedMethods());

            kernelCompResult.addCompiledMethodCode(compResult.getTargetCode());
        }

        return kernelCompResult;
    }

    public static OCLCompilationResult compileSketchForDevice(Sketch sketch, CompilableTask task, OCLProviders providers, OCLBackend backend) {
        final StructuredGraph kernelGraph = (StructuredGraph) sketch.getGraph().getReadonlyCopy().copy(getDebugContext());
        ResolvedJavaMethod resolvedMethod = kernelGraph.method();

        info("Compiling sketch %s on %s", resolvedMethod.getName(), backend.getDeviceContext().getDevice().getDeviceName());

        final TaskMetaData taskMeta = task.meta();
        final Object[] args = task.getArguments();
        final long batchThreads = (taskMeta.getNumThreads() > 0) ? taskMeta.getNumThreads() : task.getBatchThreads();
        taskMeta.setCompiledGraph(resolvedMethod);

        OptimisticOptimizations optimisticOpts = OptimisticOptimizations.ALL;
        ProfilingInfo profilingInfo = resolvedMethod.getProfilingInfo();

        String kernelName = OCLDeviceContext.checkKernelName(resolvedMethod.getName());
        OCLCompilationResult kernelCompResult = new OCLCompilationResult(task.getId(), kernelName, taskMeta, backend);
        CompilationResultBuilderFactory factory = CompilationResultBuilderFactory.Default;

        Set<ResolvedJavaMethod> methods = new HashSet<>();

        final OCLSuitesProvider suitesProvider = providers.getSuitesProvider();
        Request<OCLCompilationResult> kernelCompilationRequest = new Request<>(kernelGraph, resolvedMethod, args, taskMeta, providers, backend, suitesProvider.getGraphBuilderSuite(), optimisticOpts,
                profilingInfo, suitesProvider.getSuites(), suitesProvider.getLIRSuites(), kernelCompResult, factory, true, false, batchThreads);

        kernelCompilationRequest.execute();

        if (DUMP_COMPILED_METHODS) {
            methods.add(kernelGraph.method());
            methods.addAll(kernelGraph.getMethods());
            Collections.addAll(methods, kernelCompResult.getMethods());
        }

        /*
         * Given the non-inlined methods A, B, C, D and the call graph below, method D can be compiled twice.
         * A  → B → D
         *    ↘ C ↗
         * We use hash set below to prevent this.
         */
        final Set<ResolvedJavaMethod> nonInlinedCompiledMethods = new HashSet<>();
        final Deque<ResolvedJavaMethod> workList = new ArrayDeque<>(kernelCompResult.getNonInlinedMethods());
        while (!workList.isEmpty()) {
            final ResolvedJavaMethod currentMethod = workList.pop();
            if (nonInlinedCompiledMethods.contains(currentMethod)) {
                continue;
            } else {
                nonInlinedCompiledMethods.add(currentMethod);
            }
            Sketch currentSketch = TornadoSketcher.lookup(currentMethod, task.meta().getDriverIndex(), task.meta().getDeviceIndex());
            final StructuredGraph graph = (StructuredGraph) currentSketch.getGraph().getMutableCopy(null);

            String subKernelName = OCLDeviceContext.checkKernelName(currentMethod.getName());
            final OCLCompilationResult compResult = new OCLCompilationResult(task.getId(), subKernelName, taskMeta, backend);

            Request<OCLCompilationResult> methodCompilationRequest = new Request<>(graph, currentMethod, //
                    null, null, providers, backend, suitesProvider.getGraphBuilderSuite(), //
                    optimisticOpts, profilingInfo, suitesProvider.getSuites(), suitesProvider.getLIRSuites(), //
                    compResult, factory, false, false, 0);

            methodCompilationRequest.execute();
            workList.addAll(compResult.getNonInlinedMethods());

            if (DUMP_COMPILED_METHODS) {
                methods.add(graph.method());
                methods.addAll(graph.getMethods());
            }

            kernelCompResult.addCompiledMethodCode(compResult.getTargetCode());
        }

        if (DUMP_COMPILED_METHODS) {
            final Path outDir = Paths.get("./opencl-compiled-methods");
            if (!Files.exists(outDir)) {
                try {
                    Files.createDirectories(outDir);
                } catch (IOException e) {
                    error("unable to create cache dir: %s", outDir.toString());
                    error(e.getMessage());
                }
            }

            guarantee(Files.isDirectory(outDir), "cache directory is not a directory: %s", outDir.toAbsolutePath().toString());

            File file = new File(outDir + "/" + task.getId() + "-" + resolvedMethod.getName());
            try (PrintWriter pw = new PrintWriter(file)) {
                for (ResolvedJavaMethod m : methods) {
                    pw.printf("%s,%s\n", m.getDeclaringClass().getName(), m.getName());
                }
            } catch (IOException e) {
                error("unable to dump source: ", e.getMessage());
            }
        }

        return kernelCompResult;
    }
}
