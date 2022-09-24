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

package uk.ac.manchester.tornado.drivers.ptx.graal.compiler;

import jdk.vm.ci.code.TargetDescription;
import jdk.vm.ci.meta.Assumptions;
import jdk.vm.ci.meta.ProfilingInfo;
import jdk.vm.ci.meta.ResolvedJavaMethod;
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
import org.graalvm.compiler.lir.phases.AllocationPhase;
import org.graalvm.compiler.lir.phases.PreAllocationOptimizationPhase;
import org.graalvm.compiler.nodes.StructuredGraph;
import org.graalvm.compiler.nodes.cfg.Block;
import org.graalvm.compiler.nodes.spi.NodeLIRBuilderTool;
import org.graalvm.compiler.options.OptionValues;
import org.graalvm.compiler.phases.OptimisticOptimizations;
import org.graalvm.compiler.phases.PhaseSuite;
import org.graalvm.compiler.phases.common.DeadCodeEliminationPhase;
import org.graalvm.compiler.phases.tiers.HighTierContext;
import org.graalvm.compiler.phases.util.Providers;
import uk.ac.manchester.tornado.drivers.ptx.graal.PTXProviders;
import uk.ac.manchester.tornado.drivers.ptx.graal.PTXSuitesProvider;
import uk.ac.manchester.tornado.drivers.ptx.graal.backend.PTXBackend;
import uk.ac.manchester.tornado.drivers.ptx.graal.nodes.PrintfNode;
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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.graalvm.compiler.phases.common.DeadCodeEliminationPhase.Optionality.Optional;
import static uk.ac.manchester.tornado.api.exceptions.TornadoInternalError.guarantee;
import static uk.ac.manchester.tornado.drivers.ptx.graal.PTXCodeUtil.buildKernelName;
import static uk.ac.manchester.tornado.drivers.ptx.graal.compiler.PTXLIRGenerationPhase.*;
import static uk.ac.manchester.tornado.runtime.TornadoCoreRuntime.getDebugContext;
import static uk.ac.manchester.tornado.runtime.TornadoCoreRuntime.getTornadoRuntime;
import static uk.ac.manchester.tornado.runtime.common.Tornado.*;
import static uk.ac.manchester.tornado.runtime.common.Tornado.error;

public class PTXCompiler {

    private static final AtomicInteger compilationId = new AtomicInteger();

    private static final TimerKey CompilerTimer = DebugContext.timer("PTXGraalCompiler");
    private static final TimerKey FrontEnd = DebugContext.timer("PTXFrontend");
    private static final TimerKey BackEnd = DebugContext.timer("PTXBackend");
    private static final TimerKey EmitLIR = DebugContext.timer("PTXEmitLIR");
    private static final TimerKey EmitCode = DebugContext.timer("PTXEmitCode");

    private static final PTXLIRGenerationPhase LIR_GENERATION_PHASE = new PTXLIRGenerationPhase();

    public static class PTXCompilationRequest {
        public final StructuredGraph graph;
        public final ResolvedJavaMethod installedCodeOwner;
        public final Object[] args;
        public final TaskMetaData meta;
        public final Providers providers;
        public final PTXBackend backend;
        public final PhaseSuite<HighTierContext> graphBuilderSuite;
        public final OptimisticOptimizations optimisticOpts;
        public final ProfilingInfo profilingInfo;
        public final TornadoSuites suites;
        public final TornadoLIRSuites lirSuites;
        public final PTXCompilationResult compilationResult;
        public final CompilationResultBuilderFactory factory;
        public final boolean isKernel;
        public final boolean buildGraph;
        public final long batchThreads;
        public final boolean includePrintf;

        private PTXCompilationRequest(StructuredGraph graph, ResolvedJavaMethod installedCodeOwner, Object[] args, TaskMetaData meta, Providers providers, PTXBackend backend,
                PhaseSuite<HighTierContext> graphBuilderSuite, OptimisticOptimizations optimisticOpts, ProfilingInfo profilingInfo, TornadoSuites suites, TornadoLIRSuites lirSuites,
                PTXCompilationResult compilationResult, CompilationResultBuilderFactory factory, boolean isKernel, boolean buildGraph, long batchThreads, boolean includePrintf) {
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
            this.includePrintf = includePrintf;
        }

        public PTXCompilationResult execute() {
            return PTXCompiler.compile(this);
        }

        // FIXME <REFACTOR> this class can be merged into PTXCompilationRequest
        public static class PTXCompilationRequestBuilder {
            private StructuredGraph graph;
            private ResolvedJavaMethod codeOwner;
            private Object[] args;
            private TaskMetaData meta;
            private Providers providers;
            private PTXBackend backend;
            private PhaseSuite<HighTierContext> graphBuilderSuite;
            private OptimisticOptimizations optimisticOpts;
            private ProfilingInfo profilingInfo;
            private TornadoSuites suites;
            private TornadoLIRSuites lirSuites;
            private PTXCompilationResult compilationResult;
            private CompilationResultBuilderFactory factory;
            private boolean isKernel;
            private boolean buildGraph;
            private long batchThreads;
            private boolean includePrintf;

            private PTXCompilationRequestBuilder() {
            }

            public PTXCompilationRequest build() {
                return new PTXCompilationRequest(graph, codeOwner, args, meta, providers, backend, graphBuilderSuite, optimisticOpts, profilingInfo, suites, lirSuites, compilationResult, factory,
                        isKernel, buildGraph, batchThreads, includePrintf);
            }

            public static PTXCompilationRequestBuilder getInstance() {
                return new PTXCompilationRequestBuilder();
            }

            public PTXCompilationRequestBuilder withGraph(StructuredGraph graph) {
                this.graph = graph;
                return this;
            }

            public PTXCompilationRequestBuilder withCodeOwner(ResolvedJavaMethod owner) {
                this.codeOwner = owner;
                return this;
            }

            public PTXCompilationRequestBuilder withArgs(Object[] args) {
                this.args = args;
                return this;
            }

            public PTXCompilationRequestBuilder withMetaData(TaskMetaData metaData) {
                this.meta = metaData;
                return this;
            }

            public PTXCompilationRequestBuilder withProviders(Providers providers) {
                this.providers = providers;
                return this;
            }

            public PTXCompilationRequestBuilder withBackend(PTXBackend backend) {
                this.backend = backend;
                return this;
            }

            public PTXCompilationRequestBuilder withGraphBuilderSuite(PhaseSuite<HighTierContext> builderSuite) {
                this.graphBuilderSuite = builderSuite;
                return this;
            }

            public PTXCompilationRequestBuilder withOptimizations(OptimisticOptimizations optimizations) {
                this.optimisticOpts = optimizations;
                return this;
            }

            public PTXCompilationRequestBuilder withProfilingInfo(ProfilingInfo profilingInfo) {
                this.profilingInfo = profilingInfo;
                return this;
            }

            public PTXCompilationRequestBuilder withSuites(TornadoSuites suites) {
                this.suites = suites;
                return this;
            }

            public PTXCompilationRequestBuilder withLIRSuites(TornadoLIRSuites suites) {
                this.lirSuites = suites;
                return this;
            }

            public PTXCompilationRequestBuilder withResult(PTXCompilationResult result) {
                this.compilationResult = result;
                return this;
            }

            public PTXCompilationRequestBuilder withResultBuilderFactory(CompilationResultBuilderFactory factory) {
                this.factory = factory;
                return this;
            }

            public PTXCompilationRequestBuilder isKernel(boolean isKernel) {
                this.isKernel = isKernel;
                return this;
            }

            public PTXCompilationRequestBuilder buildGraph(boolean buildGraph) {
                this.buildGraph = buildGraph;
                return this;
            }

            public PTXCompilationRequestBuilder withBatchThreads(long batchThreads) {
                this.batchThreads = batchThreads;
                return this;
            }

            public PTXCompilationRequestBuilder includePrintf(boolean includePrintf) {
                this.includePrintf = includePrintf;
                return this;
            }
        }
    }

    private static PTXCompilationResult compile(PTXCompilationRequest r) {
        assert !r.graph.isFrozen();
        try (DebugContext.Scope s0 = getDebugContext().scope("GraalCompiler", r.graph, r.providers.getCodeCache()); DebugCloseable a = CompilerTimer.start(getDebugContext())) {
            emitFrontEnd(r);
            boolean isParallel = false;
            if (r.meta != null && r.meta.isParallel()) {
                isParallel = true;
            }
            emitBackEnd(r, isParallel);
        } catch (Throwable e) {
            throw getDebugContext().handle(e);
        }

        return r.compilationResult;
    }

    private static void emitBackEnd(PTXCompilationRequest r, boolean isParallel) {
        try (DebugContext.Scope s = getDebugContext().scope("PTXBackend", r.graph.getLastSchedule()); DebugCloseable a = BackEnd.start(getDebugContext())) {
            LIRGenerationResult lirGen = emitLIR(r);
            try (DebugContext.Scope s2 = getDebugContext().scope("PTXCodeGen", lirGen, lirGen.getLIR())) {
                r.compilationResult.setHasUnsafeAccess(r.graph.hasUnsafeAccess());
                emitCode(r, lirGen, isParallel);
            } catch (Throwable e) {
                throw getDebugContext().handle(e);
            }
        } catch (Throwable e) {
            throw getDebugContext().handle(e);
        }
    }

    private static void emitCode(PTXCompilationRequest r, LIRGenerationResult lirGenRes, boolean isParallel) {
        try (DebugCloseable a = EmitCode.start(getDebugContext())) {
            FrameMap frameMap = lirGenRes.getFrameMap();
            final PTXCompilationResultBuilder crb = r.backend.newCompilationResultBuilder(lirGenRes, frameMap, r.compilationResult, r.factory, r.isKernel, isParallel, r.includePrintf);
            r.backend.emitCode(crb, ((PTXLIRGenerationResult) lirGenRes), r.installedCodeOwner);

            Assumptions assumptions = r.graph.getAssumptions();
            if (assumptions != null && !assumptions.isEmpty()) {
                r.compilationResult.setAssumptions(assumptions.toArray());
            }
            Collection<ResolvedJavaMethod> inlinedMethods = r.graph.getMethods();
            if (inlinedMethods != null) {
                r.compilationResult.setMethods(r.installedCodeOwner, inlinedMethods);
            }

            r.compilationResult.setNonInlinedMethods(crb.getNonInlinedMethods());
            crb.finish();

            if (getDebugContext().isCountEnabled()) {
                DebugContext.counter("CompilationResults").increment(getDebugContext());
                DebugContext.counter("CodeBytesEmitted").add(getDebugContext(), r.compilationResult.getTargetCodeSize());
            }

            getDebugContext().dump(DebugContext.BASIC_LEVEL, r.compilationResult, "After code generation");
        }
    }

    private static LIRGenerationResult emitLIR(PTXCompilationRequest r) {
        try (DebugContext.Scope ds = getDebugContext().scope("EmitLIR"); DebugCloseable a = EmitLIR.start(getDebugContext())) {
            OptionValues options = r.graph.getOptions();
            StructuredGraph.ScheduleResult schedule = r.graph.getLastSchedule();
            Block[] blocks = schedule.getCFG().getBlocks();
            Block startBlock = schedule.getCFG().getStartBlock();
            assert startBlock != null;
            assert startBlock.getPredecessorCount() == 0;

            LIR lir = null;
            AbstractBlockBase<?>[] codeEmittingOrder;
            AbstractBlockBase<?>[] linearScanOrder;
            try (DebugContext.Scope s = getDebugContext().scope("ComputeLinearScanOrder", lir)) {
                codeEmittingOrder = ComputeBlockOrder.computeCodeEmittingOrder(blocks.length, startBlock);
                linearScanOrder = ComputeBlockOrder.computeLinearScanOrder(blocks.length, startBlock);

                lir = new LIR(schedule.getCFG(), linearScanOrder, codeEmittingOrder, options, getDebugContext());
                getDebugContext().dump(DebugContext.INFO_LEVEL, lir, "After linear scan order");
            } catch (Throwable e) {
                throw getDebugContext().handle(e);
            }
            RegisterAllocationConfig registerAllocationConfig = r.backend.newRegisterAllocationConfig(null, new String[] {});
            FrameMapBuilder frameMapBuilder = r.backend.newFrameMapBuilder(null);
            LIRGenerationResult lirGenRes = r.backend.newLIRGenerationResult(r.graph.compilationId(), lir, frameMapBuilder, registerAllocationConfig);
            LIRGeneratorTool lirGen = r.backend.newLIRGenerator(lirGenRes);
            NodeLIRBuilderTool nodeLirGen = r.backend.newNodeLIRBuilder(r.graph, lirGen);

            // LIR generation
            LIRGenerationContext context = new LIRGenerationContext(lirGen, nodeLirGen, r.graph, schedule, r.isKernel);
            LIR_GENERATION_PHASE.apply(r.backend.getTarget(), lirGenRes, context);

            try (DebugContext.Scope s = getDebugContext().scope("LIRStages", nodeLirGen, lir)) {
                getDebugContext().dump(DebugContext.BASIC_LEVEL, lir, "After LIR generation");
                LIRGenerationResult result = emitLowLevel(r.backend.getTarget(), lirGenRes, lirGen, r.lirSuites, registerAllocationConfig);
                getDebugContext().dump(DebugContext.BASIC_LEVEL, lir, "Before code generation");
                return result;
            } catch (Throwable e) {
                throw getDebugContext().handle(e);
            }
        } catch (Throwable e) {
            throw getDebugContext().handle(e);
        }
    }

    private static LIRGenerationResult emitLowLevel(TargetDescription target, LIRGenerationResult lirGenRes, LIRGeneratorTool lirGen, TornadoLIRSuites lirSuites,
            RegisterAllocationConfig registerAllocationConfig) {
        final PreAllocationOptimizationPhase.PreAllocationOptimizationContext preAllocOptContext = new PreAllocationOptimizationPhase.PreAllocationOptimizationContext(lirGen);
        lirSuites.getPreAllocationStage().apply(target, lirGenRes, preAllocOptContext);
        AllocationPhase.AllocationContext allocContext = new AllocationPhase.AllocationContext(lirGen.getSpillMoveFactory(), registerAllocationConfig);
        lirSuites.getAllocationStage().apply(target, lirGenRes, allocContext);
        return lirGenRes;
    }

    /**
     * Builds the graph and optimizes it.
     */
    private static void emitFrontEnd(PTXCompilationRequest r) {
        try (DebugContext.Scope s = getDebugContext().scope("PTXFrontend", new DebugDumpScope("PTXFrontend")); DebugCloseable a = FrontEnd.start(getDebugContext())) {
            final TornadoHighTierContext highTierContext = new TornadoHighTierContext(r.providers, r.graphBuilderSuite, r.optimisticOpts, r.installedCodeOwner, r.args, r.meta, r.isKernel,
                    r.batchThreads);

            if (r.buildGraph) {
                if (isGraphEmpty(r.graph)) {
                    r.graphBuilderSuite.apply(r.graph, highTierContext);
                    new DeadCodeEliminationPhase(Optional).apply(r.graph);
                } else {
                    getDebugContext().dump(DebugContext.INFO_LEVEL, r.graph, "initial state");
                }
            }
            r.suites.getHighTier().apply(r.graph, highTierContext);
            r.graph.maybeCompress();

            final TornadoMidTierContext midTierContext = new TornadoMidTierContext(r.providers, r.backend, r.optimisticOpts, r.profilingInfo, r.installedCodeOwner, r.args, r.meta);
            r.suites.getMidTier().apply(r.graph, midTierContext);

            r.graph.maybeCompress();

            final TornadoLowTierContext lowTierContext = new TornadoLowTierContext(r.providers, r.backend, r.meta);
            r.suites.getLowTier().apply(r.graph, lowTierContext);

            getDebugContext().dump(DebugContext.BASIC_LEVEL, r.graph.getLastSchedule(), "Final HIR schedule");
        } catch (Throwable e) {
            throw getDebugContext().handle(e);
        }
    }

    private static boolean isGraphEmpty(StructuredGraph graph) {
        return graph.start().next() == null;
    }

    public static PTXCompilationResult compileSketchForDevice(Sketch sketch, CompilableTask task, PTXProviders providers, PTXBackend backend) {
        final StructuredGraph kernelGraph = (StructuredGraph) sketch.getGraph().getReadonlyCopy().copy(getDebugContext());
        ResolvedJavaMethod resolvedMethod = kernelGraph.method();

        info("Compiling sketch %s on %s", resolvedMethod.getName(), backend.getDeviceContext().getDevice().getDeviceName());

        final TaskMetaData taskMeta = task.meta();
        final Object[] args = task.getArguments();
        final long batchThreads = (taskMeta.getNumThreads() > 0) ? taskMeta.getNumThreads() : task.getBatchThreads();

        OptimisticOptimizations optimisticOpts = OptimisticOptimizations.ALL;
        ProfilingInfo profilingInfo = resolvedMethod.getProfilingInfo();

        PTXCompilationResult kernelCompResult = new PTXCompilationResult(buildKernelName(resolvedMethod.getName(), task));
        CompilationResultBuilderFactory factory = CompilationResultBuilderFactory.Default;

        Set<ResolvedJavaMethod> methods = new HashSet<>();
        boolean includePrintf = kernelGraph.hasNode(PrintfNode.TYPE);

        final PTXSuitesProvider suitesProvider = (PTXSuitesProvider) providers.getSuitesProvider();
        PTXCompilationRequest kernelCompilationRequest = PTXCompilationRequest.PTXCompilationRequestBuilder.getInstance().withGraph(kernelGraph).withCodeOwner(resolvedMethod).withArgs(args)
                .withMetaData(taskMeta).withProviders(providers).withBackend(backend).withGraphBuilderSuite(suitesProvider.getGraphBuilderSuite()).withOptimizations(optimisticOpts)
                .withProfilingInfo(profilingInfo).withSuites(suitesProvider.getSuites()).withLIRSuites(suitesProvider.getLIRSuites()).withResult(kernelCompResult).withResultBuilderFactory(factory)
                .isKernel(true).buildGraph(true).includePrintf(includePrintf).withBatchThreads(batchThreads).build();

        kernelCompilationRequest.execute();

        if (DUMP_COMPILED_METHODS) {
            methods.add(kernelGraph.method());
            methods.addAll(kernelGraph.getMethods());
            methods.addAll(Arrays.asList(kernelCompResult.getMethods()));
        }

        /*
         * Given the non-inlined methods A, B, C, D and the call graph below, method D can be compiled twice.
         * A  → B → D
         *    ↘ C ↗
         * We use hash set below to prevent this.
         */
        final Set<ResolvedJavaMethod> nonInlinedCompiledMethods = new HashSet<>();
        final Deque<ResolvedJavaMethod> worklist = new ArrayDeque<>(kernelCompResult.getNonInlinedMethods());
        while (!worklist.isEmpty()) {
            final ResolvedJavaMethod currentMethod = worklist.pop();
            if (nonInlinedCompiledMethods.contains(currentMethod)) {
                continue;
            } else {
                nonInlinedCompiledMethods.add(currentMethod);
            }
            Sketch currentSketch = TornadoSketcher.lookup(currentMethod, task.meta().getDriverIndex(), task.meta().getDeviceIndex());
            final PTXCompilationResult compResult = new PTXCompilationResult(currentMethod.getName());
            final StructuredGraph graph = (StructuredGraph) currentSketch.getGraph().getMutableCopy(null);

            PTXCompilationRequest methodCompilationRequest = PTXCompilationRequest.PTXCompilationRequestBuilder.getInstance().withGraph(graph).withCodeOwner(currentMethod).withProviders(providers)
                    .withBackend(backend).withGraphBuilderSuite(suitesProvider.getGraphBuilderSuite()).withOptimizations(optimisticOpts).withProfilingInfo(profilingInfo)
                    .withSuites(suitesProvider.getSuites()).withLIRSuites(suitesProvider.getLIRSuites()).withResult(compResult).withResultBuilderFactory(factory).isKernel(false).buildGraph(false)
                    .includePrintf(false).withBatchThreads(0).build();

            methodCompilationRequest.execute();
            worklist.addAll(compResult.getNonInlinedMethods());

            if (DUMP_COMPILED_METHODS) {
                methods.add(graph.method());
                methods.addAll(graph.getMethods());
            }

            kernelCompResult.addCompiledMethodCode(compResult.getTargetCode());
        }

        kernelCompResult.addPTXHeader(backend);

        if (DUMP_COMPILED_METHODS) {
            final Path outDir = Paths.get("./ptx-compiled-methods");
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

    public static PTXCompilationResult compileCodeForDevice(ResolvedJavaMethod resolvedMethod, Object[] args, TaskMetaData meta, PTXProviders providers, PTXBackend backend, long batchThreads) {
        Tornado.info("Compiling %s on %s", resolvedMethod.getName(), backend.getDeviceContext().getDevice().getDeviceName());
        final TornadoCompilerIdentifier id = new TornadoCompilerIdentifier("compile-kernel" + resolvedMethod.getName(), compilationId.getAndIncrement());

        StructuredGraph.Builder builder = new StructuredGraph.Builder(getTornadoRuntime().getOptions(), getDebugContext(), StructuredGraph.AllowAssumptions.YES);
        builder.method(resolvedMethod);
        builder.compilationId(id);
        builder.name("compile-kernel" + resolvedMethod.getName());

        final StructuredGraph kernelGraph = builder.build();

        OptimisticOptimizations optimisticOpts = OptimisticOptimizations.ALL;
        ProfilingInfo profilingInfo = resolvedMethod.getProfilingInfo();

        PTXCompilationResult kernelCompResult = new PTXCompilationResult(resolvedMethod.getName());
        CompilationResultBuilderFactory factory = CompilationResultBuilderFactory.Default;

        final PTXSuitesProvider suitesProvider = (PTXSuitesProvider) providers.getSuitesProvider();
        PTXCompilationRequest kernelCompilationRequest = PTXCompilationRequest.PTXCompilationRequestBuilder.getInstance().withGraph(kernelGraph).withCodeOwner(resolvedMethod).withArgs(args)
                .withMetaData(meta).withProviders(providers).withBackend(backend).withGraphBuilderSuite(suitesProvider.getGraphBuilderSuite()).withOptimizations(optimisticOpts)
                .withProfilingInfo(profilingInfo).withSuites(suitesProvider.getSuites()).withLIRSuites(suitesProvider.getLIRSuites()).withResult(kernelCompResult).withResultBuilderFactory(factory)
                .isKernel(true).buildGraph(true).includePrintf(false).withBatchThreads(batchThreads).build();

        kernelCompilationRequest.execute();

        final Deque<ResolvedJavaMethod> workList = new ArrayDeque<>(kernelCompResult.getNonInlinedMethods());

        while (!workList.isEmpty()) {
            final ResolvedJavaMethod currentMethod = workList.pop();
            final PTXCompilationResult compResult = new PTXCompilationResult(currentMethod.getName());
            StructuredGraph.Builder builder1 = new StructuredGraph.Builder(TornadoCoreRuntime.getOptions(), getDebugContext(), StructuredGraph.AllowAssumptions.YES);
            builder1.method(resolvedMethod);
            builder1.compilationId(id);
            builder1.name("internal" + currentMethod.getName());

            final StructuredGraph graph = builder.build();
            PTXCompilationRequest methodCompilationRequest = PTXCompilationRequest.PTXCompilationRequestBuilder.getInstance().withGraph(graph).withCodeOwner(currentMethod).withProviders(providers)
                    .withBackend(backend).withGraphBuilderSuite(suitesProvider.getGraphBuilderSuite()).withOptimizations(optimisticOpts).withProfilingInfo(profilingInfo)
                    .withSuites(suitesProvider.getSuites()).withLIRSuites(suitesProvider.getLIRSuites()).withResult(compResult).withResultBuilderFactory(factory).isKernel(false).buildGraph(true)
                    .includePrintf(false).withBatchThreads(0).build();

            methodCompilationRequest.execute();
            workList.addAll(compResult.getNonInlinedMethods());

            kernelCompResult.addCompiledMethodCode(compResult.getTargetCode());
        }

        kernelCompResult.addPTXHeader(backend);

        return kernelCompResult;
    }
}
