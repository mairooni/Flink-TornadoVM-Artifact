/*
 * This file is part of Tornado: A heterogeneous programming framework:
 * https://github.com/beehive-lab/tornadovm
 *
 * Copyright (c) 2021, APT Group, Department of Computer Science,
 * School of Engineering, The University of Manchester. All rights reserved.
 * Copyright (c) 2009-2021, Oracle and/or its affiliates. All rights reserved.
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
package uk.ac.manchester.tornado.drivers.spirv.graal;

import static org.graalvm.compiler.nodes.NamedLocationIdentity.ARRAY_LENGTH_LOCATION;
import static uk.ac.manchester.tornado.api.exceptions.TornadoInternalError.shouldNotReachHere;
import static uk.ac.manchester.tornado.api.exceptions.TornadoInternalError.unimplemented;

import java.util.Iterator;

import org.graalvm.compiler.api.replacements.SnippetReflectionProvider;
import org.graalvm.compiler.core.common.spi.ForeignCallsProvider;
import org.graalvm.compiler.core.common.spi.MetaAccessExtensionProvider;
import org.graalvm.compiler.core.common.type.ObjectStamp;
import org.graalvm.compiler.core.common.type.Stamp;
import org.graalvm.compiler.core.common.type.StampFactory;
import org.graalvm.compiler.core.common.type.StampPair;
import org.graalvm.compiler.debug.DebugHandlersFactory;
import org.graalvm.compiler.graph.Node;
import org.graalvm.compiler.graph.NodeInputList;
import org.graalvm.compiler.nodes.AbstractDeoptimizeNode;
import org.graalvm.compiler.nodes.CompressionNode;
import org.graalvm.compiler.nodes.ConstantNode;
import org.graalvm.compiler.nodes.FixedNode;
import org.graalvm.compiler.nodes.Invoke;
import org.graalvm.compiler.nodes.InvokeNode;
import org.graalvm.compiler.nodes.LoweredCallTargetNode;
import org.graalvm.compiler.nodes.NamedLocationIdentity;
import org.graalvm.compiler.nodes.NodeView;
import org.graalvm.compiler.nodes.PhiNode;
import org.graalvm.compiler.nodes.StructuredGraph;
import org.graalvm.compiler.nodes.UnwindNode;
import org.graalvm.compiler.nodes.ValueNode;
import org.graalvm.compiler.nodes.calc.FloatConvertNode;
import org.graalvm.compiler.nodes.calc.IntegerDivRemNode;
import org.graalvm.compiler.nodes.calc.RemNode;
import org.graalvm.compiler.nodes.java.ArrayLengthNode;
import org.graalvm.compiler.nodes.java.InstanceOfNode;
import org.graalvm.compiler.nodes.java.LoadFieldNode;
import org.graalvm.compiler.nodes.java.LoadIndexedNode;
import org.graalvm.compiler.nodes.java.MethodCallTargetNode;
import org.graalvm.compiler.nodes.java.StoreFieldNode;
import org.graalvm.compiler.nodes.java.StoreIndexedNode;
import org.graalvm.compiler.nodes.memory.AbstractWriteNode;
import org.graalvm.compiler.nodes.memory.OnHeapMemoryAccess;
import org.graalvm.compiler.nodes.memory.ReadNode;
import org.graalvm.compiler.nodes.memory.WriteNode;
import org.graalvm.compiler.nodes.memory.address.AddressNode;
import org.graalvm.compiler.nodes.memory.address.OffsetAddressNode;
import org.graalvm.compiler.nodes.spi.LoweringTool;
import org.graalvm.compiler.nodes.spi.PlatformConfigurationProvider;
import org.graalvm.compiler.nodes.type.StampTool;
import org.graalvm.compiler.nodes.util.GraphUtil;
import org.graalvm.compiler.options.OptionValues;
import org.graalvm.compiler.phases.util.Providers;
import org.graalvm.compiler.replacements.DefaultJavaLoweringProvider;
import org.graalvm.compiler.replacements.SnippetCounter;

import jdk.vm.ci.hotspot.HotSpotCallingConventionType;
import jdk.vm.ci.hotspot.HotSpotResolvedJavaField;
import jdk.vm.ci.meta.ConstantReflectionProvider;
import jdk.vm.ci.meta.JavaConstant;
import jdk.vm.ci.meta.JavaKind;
import jdk.vm.ci.meta.JavaType;
import jdk.vm.ci.meta.MetaAccessProvider;
import jdk.vm.ci.meta.PrimitiveConstant;
import jdk.vm.ci.meta.ResolvedJavaField;
import jdk.vm.ci.meta.ResolvedJavaType;
import uk.ac.manchester.tornado.drivers.spirv.SPIRVTargetDescription;
import uk.ac.manchester.tornado.drivers.spirv.graal.lir.SPIRVKind;
import uk.ac.manchester.tornado.drivers.spirv.graal.nodes.CastNode;
import uk.ac.manchester.tornado.drivers.spirv.graal.nodes.FixedArrayNode;
import uk.ac.manchester.tornado.drivers.spirv.graal.nodes.GlobalThreadIdNode;
import uk.ac.manchester.tornado.drivers.spirv.graal.nodes.GlobalThreadSizeNode;
import uk.ac.manchester.tornado.drivers.spirv.graal.nodes.GroupIdNode;
import uk.ac.manchester.tornado.drivers.spirv.graal.nodes.LocalArrayNode;
import uk.ac.manchester.tornado.drivers.spirv.graal.nodes.LocalThreadIdNode;
import uk.ac.manchester.tornado.drivers.spirv.graal.nodes.LocalThreadSizeNode;
import uk.ac.manchester.tornado.drivers.spirv.graal.phases.TornadoFloatingReadReplacement;
import uk.ac.manchester.tornado.drivers.spirv.graal.snippets.ReduceGPUSnippets;
import uk.ac.manchester.tornado.runtime.TornadoVMConfig;
import uk.ac.manchester.tornado.runtime.graal.nodes.GetGroupIdFixedWithNextNode;
import uk.ac.manchester.tornado.runtime.graal.nodes.GlobalGroupSizeFixedWithNextNode;
import uk.ac.manchester.tornado.runtime.graal.nodes.LocalGroupSizeFixedWithNextNode;
import uk.ac.manchester.tornado.runtime.graal.nodes.NewArrayNonVirtualizableNode;
import uk.ac.manchester.tornado.runtime.graal.nodes.StoreAtomicIndexedNode;
import uk.ac.manchester.tornado.runtime.graal.nodes.ThreadIdFixedWithNextNode;
import uk.ac.manchester.tornado.runtime.graal.nodes.ThreadLocalIdFixedWithNextNode;
import uk.ac.manchester.tornado.runtime.graal.nodes.TornadoDirectCallTargetNode;
import uk.ac.manchester.tornado.runtime.graal.phases.MarkLocalArray;

public class SPIRVLoweringProvider extends DefaultJavaLoweringProvider {

    private static final TornadoFloatingReadReplacement snippetReadReplacementPhase = new TornadoFloatingReadReplacement(true, true);

    private ConstantReflectionProvider constantReflectionProvider;
    private TornadoVMConfig vmConfig;

    private static boolean gpuSnippet = false;
    private ReduceGPUSnippets.Templates GPUReduceSnippets;

    public SPIRVLoweringProvider(MetaAccessProvider metaAccess, ForeignCallsProvider foreignCalls, PlatformConfigurationProvider platformConfig,
            MetaAccessExtensionProvider metaAccessExtensionProvider, ConstantReflectionProvider constantReflectionProvider, TornadoVMConfig vmConfig, SPIRVTargetDescription target,
            boolean useCompressedOops) {
        super(metaAccess, foreignCalls, platformConfig, metaAccessExtensionProvider, target, useCompressedOops);
        this.constantReflectionProvider = constantReflectionProvider;
        this.vmConfig = vmConfig;
    }

    public static boolean isGPUSnippet() {
        return gpuSnippet;
    }

    @Override
    public void initialize(OptionValues options, Iterable<DebugHandlersFactory> debugHandlersFactories, SnippetCounter.Group.Factory factory, Providers providers,
            SnippetReflectionProvider snippetReflection) {
        super.initialize(options, debugHandlersFactories, factory, providers, snippetReflection);
        initializeSnippets(options, debugHandlersFactories, factory, providers, snippetReflection);
    }

    private void initializeSnippets(OptionValues options, Iterable<DebugHandlersFactory> debugHandlersFactories, SnippetCounter.Group.Factory factory, Providers providers,
            SnippetReflectionProvider snippetReflection) {
        this.GPUReduceSnippets = new ReduceGPUSnippets.Templates(options, debugHandlersFactories, providers, snippetReflection, target);
    }

    private boolean shouldIgnoreNode(Node node) {
        return (node instanceof AbstractDeoptimizeNode //
                || node instanceof UnwindNode //
                || node instanceof RemNode //
                || node instanceof InstanceOfNode //
                || node instanceof IntegerDivRemNode);
    }

    @Override
    public void lower(Node node, LoweringTool tool) {
        if (shouldIgnoreNode(node)) {
            return;
        }
        if (node instanceof Invoke) {
            lowerInvoke((Invoke) node, tool, (StructuredGraph) node.graph());
        } else if (node instanceof LoadIndexedNode) {
            lowerLoadIndexedNode((LoadIndexedNode) node, tool);
        } else if (node instanceof NewArrayNonVirtualizableNode) {
            lowerNewArrayNode((NewArrayNonVirtualizableNode) node);
        } else if (node instanceof StoreIndexedNode) {
            lowerStoreIndexedNode((StoreIndexedNode) node, tool);
        } else if (node instanceof StoreAtomicIndexedNode) {
            lowerStoreAtomicsReduction(node, tool);
        } else if (node instanceof FloatConvertNode) {
            lowerFloatConvertNode((FloatConvertNode) node);
        } else if (node instanceof LoadFieldNode) {
            lowerLoadFieldNode((LoadFieldNode) node, tool);
        } else if (node instanceof StoreFieldNode) {
            lowerStoreFieldNode((StoreFieldNode) node, tool);
        } else if (node instanceof ArrayLengthNode) {
            lowerArrayLengthNode((ArrayLengthNode) node, tool);
        } else if (node instanceof ThreadIdFixedWithNextNode) {
            lowerThreadIdNode((ThreadIdFixedWithNextNode) node);
        } else if (node instanceof ThreadLocalIdFixedWithNextNode) {
            lowerLocalThreadIdNode((ThreadLocalIdFixedWithNextNode) node);
        } else if (node instanceof GetGroupIdFixedWithNextNode) {
            lowerGetGroupIdNode((GetGroupIdFixedWithNextNode) node);
        } else if (node instanceof GlobalGroupSizeFixedWithNextNode) {
            lowerGlobalGroupSizeNode((GlobalGroupSizeFixedWithNextNode) node);
        } else if (node instanceof LocalGroupSizeFixedWithNextNode) {
            lowerLocalGroupSizeNode((LocalGroupSizeFixedWithNextNode) node);
        } else {
            super.lower(node, tool);
        }
    }

    private void lowerLocalThreadIdNode(ThreadLocalIdFixedWithNextNode threadLocalIdNode) {
        StructuredGraph graph = threadLocalIdNode.graph();
        LocalThreadIdNode localThreadIdNode = graph.addWithoutUnique(new LocalThreadIdNode(ConstantNode.forInt(threadLocalIdNode.getDimension(), graph)));
        graph.replaceFixedWithFloating(threadLocalIdNode, localThreadIdNode);
    }

    private void lowerThreadIdNode(ThreadIdFixedWithNextNode threadIdNode) {
        StructuredGraph graph = threadIdNode.graph();
        GlobalThreadIdNode globalThreadIdNode = graph.addOrUnique(new GlobalThreadIdNode(ConstantNode.forInt(threadIdNode.getDimension(), graph)));
        graph.replaceFixedWithFloating(threadIdNode, globalThreadIdNode);
    }

    private void lowerReduceSnippets(StoreAtomicIndexedNode storeIndexed, LoweringTool tool) {
        StructuredGraph graph = storeIndexed.graph();

        // Find Get Global ID node and Global Size;
        GlobalThreadIdNode spirvIDNode = graph.getNodes().filter(GlobalThreadIdNode.class).first();
        GlobalThreadSizeNode spirvGlobalSize = graph.getNodes().filter(GlobalThreadSizeNode.class).first();

        ValueNode threadID = null;
        Iterator<Node> usages = spirvIDNode.usages().iterator();

        boolean cpuScheduler = false;

        while (usages.hasNext()) {
            Node n = usages.next();

            // GPU SCHEDULER
            if (n instanceof PhiNode) {
                gpuSnippet = true;
                threadID = (ValueNode) n;
                break;
            }
        }
        // Depending on the Scheduler, call the proper snippet factory
        if (cpuScheduler) {
            throw new RuntimeException("CPU Snippets for SPIR-V not implemented yet");
        } else {
            GPUReduceSnippets.lower(storeIndexed, threadID, spirvGlobalSize, tool);
        }

        // We append this phase to move floating reads close to their actual usage and
        // set FixedAccessNode::lastLocationAccess
        snippetReadReplacementPhase.apply(graph);
    }

    private void lowerStoreAtomicsReduction(Node node, LoweringTool tool) {
        StoreAtomicIndexedNode storeAtomicNode = (StoreAtomicIndexedNode) node;
        lowerReduceSnippets(storeAtomicNode, tool);
    }

    private void lowerLocalNewArray(StructuredGraph graph, int length, NewArrayNonVirtualizableNode newArray) {
        LocalArrayNode localArrayNode;
        ConstantNode newLengthNode = ConstantNode.forInt(length, graph);
        localArrayNode = graph.addWithoutUnique(new LocalArrayNode(SPIRVArchitecture.localSpace, newArray.elementType(), newLengthNode));
        newArray.replaceAtUsages(localArrayNode);
    }

    private void lowerPrivateNewArray(StructuredGraph graph, int size, NewArrayNonVirtualizableNode newArray) {
        FixedArrayNode fixedArrayNode;
        final ConstantNode newLengthNode = ConstantNode.forInt(size, graph);
        fixedArrayNode = graph.addWithoutUnique(new FixedArrayNode(SPIRVArchitecture.privateSpace, newArray.elementType(), newLengthNode));
        newArray.replaceAtUsages(fixedArrayNode);
    }

    private void lowerNewArrayNode(NewArrayNonVirtualizableNode newArray) {
        final StructuredGraph graph = newArray.graph();
        final ValueNode firstInput = newArray.length();
        if (firstInput instanceof ConstantNode) {
            if (newArray.dimensionCount() == 1) {
                final ConstantNode lengthNode = (ConstantNode) firstInput;
                if (lengthNode.getValue() instanceof PrimitiveConstant) {
                    final int length = ((PrimitiveConstant) lengthNode.getValue()).asInt();
                    if (gpuSnippet) {
                        lowerLocalNewArray(graph, length, newArray);
                    } else {
                        lowerPrivateNewArray(graph, length, newArray);
                    }
                    newArray.clearInputs();
                    GraphUtil.unlinkFixedNode(newArray);
                    GraphUtil.removeFixedWithUnusedInputs(newArray);
                } else {
                    shouldNotReachHere();
                }
            } else {
                unimplemented("multi-dimensional array declarations are not supported");
            }
        } else {
            unimplemented("dynamically sized array declarations are not supported");
        }
    }

    private void lowerGetGroupIdNode(GetGroupIdFixedWithNextNode getGroupIdNode) {
        StructuredGraph graph = getGroupIdNode.graph();
        GroupIdNode groupIdNode = graph.addOrUnique(new GroupIdNode(ConstantNode.forInt(getGroupIdNode.getDimension(), graph)));
        graph.replaceFixedWithFloating(getGroupIdNode, groupIdNode);
    }

    private void lowerGlobalGroupSizeNode(GlobalGroupSizeFixedWithNextNode globalGroupSizeNode) {
        StructuredGraph graph = globalGroupSizeNode.graph();
        GlobalThreadSizeNode globalThreadSizeNode = graph.addOrUnique(new GlobalThreadSizeNode(ConstantNode.forInt(globalGroupSizeNode.getDimension(), graph)));
        graph.replaceFixedWithFloating(globalGroupSizeNode, globalThreadSizeNode);
    }

    private void lowerLocalGroupSizeNode(LocalGroupSizeFixedWithNextNode localGroupSizeNode) {
        StructuredGraph graph = localGroupSizeNode.graph();
        LocalThreadSizeNode localThreadSizeNode = graph.addOrUnique(new LocalThreadSizeNode(ConstantNode.forInt(localGroupSizeNode.getDimension(), graph)));
        graph.replaceFixedWithFloating(localGroupSizeNode, localThreadSizeNode);
    }

    private void lowerFloatConvertNode(FloatConvertNode floatConvert) {
        final StructuredGraph graph = floatConvert.graph();
        final CastNode asFloat = graph.addWithoutUnique(new CastNode(floatConvert.stamp(NodeView.DEFAULT), floatConvert.getFloatConvert(), floatConvert.getValue()));
        floatConvert.replaceAtUsages(asFloat);
        floatConvert.safeDelete();
    }

    private void lowerInvoke(Invoke invoke, LoweringTool tool, StructuredGraph graph) {
        if (invoke.callTarget() instanceof MethodCallTargetNode) {
            MethodCallTargetNode callTarget = (MethodCallTargetNode) invoke.callTarget();
            NodeInputList<ValueNode> parameters = callTarget.arguments();
            ValueNode receiver = null;
            if (parameters.size() > 0) {
                receiver = parameters.get(0);
            }

            if (receiver != null && !callTarget.isStatic() && receiver.stamp(NodeView.DEFAULT) instanceof ObjectStamp && !StampTool.isPointerNonNull(receiver)) {
                ValueNode nonNullReceiver = createNullCheckedValue(receiver, invoke.asNode(), tool);
                parameters.set(0, nonNullReceiver);
            }

            JavaType[] signature = callTarget.targetMethod().getSignature().toParameterTypes(callTarget.isStatic() ? null : callTarget.targetMethod().getDeclaringClass());
            LoweredCallTargetNode loweredCallTarget;
            StampPair returnStampPair = callTarget.returnStamp();
            Stamp returnStamp = returnStampPair.getTrustedStamp();
            if (returnStamp instanceof ObjectStamp) {
                ObjectStamp os = (ObjectStamp) returnStamp;
                ResolvedJavaType type = os.javaType(tool.getMetaAccess());
                SPIRVKind kind = SPIRVKind.fromResolvedJavaTypeToVectorKind(type);
                if (kind != SPIRVKind.ILLEGAL) {
                    returnStampPair = StampPair.createSingle(SPIRVStampFactory.getStampFor(kind));
                }
            }

            loweredCallTarget = graph.add(new TornadoDirectCallTargetNode(parameters.toArray(new ValueNode[parameters.size()]), returnStampPair, signature, callTarget.targetMethod(),
                    HotSpotCallingConventionType.JavaCall, callTarget.invokeKind()));
            callTarget.replaceAndDelete(loweredCallTarget);
        }
    }

    @Override
    public int fieldOffset(ResolvedJavaField field) {
        HotSpotResolvedJavaField hsField = (HotSpotResolvedJavaField) field;
        return hsField.getOffset();
    }

    @Override
    public ValueNode staticFieldBase(StructuredGraph graph, ResolvedJavaField field) {
        HotSpotResolvedJavaField hsField = (HotSpotResolvedJavaField) field;
        JavaConstant base = constantReflectionProvider.asJavaClass(hsField.getDeclaringClass());
        return ConstantNode.forConstant(base, metaAccess, graph);
    }

    @Override
    public int arrayLengthOffset() {
        return vmConfig.arrayOopDescLengthOffset();
    }

    @Override
    protected Stamp loadCompressedStamp(ObjectStamp stamp) {
        unimplemented("SPIRVLoweringProvider::loadCompressedStamp");
        unimplemented();
        return null;
    }

    @Override
    protected ValueNode newCompressionNode(CompressionNode.CompressionOp op, ValueNode value) {
        unimplemented("SPIRVLoweringProvider::newCompressionNode");
        return null;
    }

    @Override
    protected ValueNode createReadHub(StructuredGraph graph, ValueNode object, LoweringTool tool) {
        unimplemented("SPIRVLoweringProvider::createReadHub unimplemented");
        return null;
    }

    @Override
    protected ValueNode createReadArrayComponentHub(StructuredGraph graph, ValueNode arrayHub, boolean isKnownObjectArray, FixedNode anchor) {
        unimplemented("SPIRVLoweringProvider::createReadArrayComponentHub unimplemented");
        return null;
    }

    /**
     * From Graal: it indicates the smallest width for comparing an integer value on
     * the target platform.
     */
    @Override
    public Integer smallestCompareWidth() {
        return null;
    }

    @Override
    public boolean supportsBulkZeroing() {
        unimplemented("SPIRVLoweringProvider::supportsBulkZeroing unimplemented");
        return false;
    }

    @Override
    public boolean supportsRounding() {
        return false;
    }

    @Override
    protected void lowerArrayLengthNode(ArrayLengthNode arrayLengthNode, LoweringTool tool) {
        StructuredGraph graph = arrayLengthNode.graph();
        ValueNode array = arrayLengthNode.array();

        AddressNode address = createOffsetAddress(graph, array, arrayLengthOffset());
        ReadNode arrayLengthRead = graph.add(new ReadNode(address, ARRAY_LENGTH_LOCATION, StampFactory.positiveInt(), OnHeapMemoryAccess.BarrierType.NONE));
        graph.replaceFixedWithFixed(arrayLengthNode, arrayLengthRead);
    }

    private boolean isLocalIDNode(LoadIndexedNode loadIndexedNode) {
        // Either the node has as input a LocalArray or has a node which will be lowered
        // to a LocalArray
        Node nd = loadIndexedNode.inputs().first().asNode();
        InvokeNode node = nd.inputs().filter(InvokeNode.class).first();
        boolean willLowerToLocalArrayNode = node != null && "Direct#NewArrayNode.newArray".equals(node.callTarget().targetName()) && gpuSnippet;
        return (nd instanceof MarkLocalArray || willLowerToLocalArrayNode);
    }

    private boolean isPrivateIDNode(LoadIndexedNode loadIndexedNode) {
        Node nd = loadIndexedNode.inputs().first().asNode();
        return (nd instanceof FixedArrayNode);
    }

    private AddressNode createArrayAccess(StructuredGraph graph, LoadIndexedNode loadIndexed, JavaKind elementKind) {
        AddressNode address;
        if (isLocalIDNode(loadIndexed) || isPrivateIDNode(loadIndexed)) {
            address = createArrayLocalAddress(graph, loadIndexed.array(), loadIndexed.index());
        } else {
            address = createArrayAddress(graph, loadIndexed.array(), elementKind, loadIndexed.index());
        }
        return address;
    }

    @Override
    public void lowerLoadIndexedNode(LoadIndexedNode loadIndexed, LoweringTool tool) {
        StructuredGraph graph = loadIndexed.graph();
        JavaKind elementKind = loadIndexed.elementKind();
        AddressNode address;

        Stamp loadStamp = loadIndexed.stamp(NodeView.DEFAULT);
        if (!(loadIndexed.stamp(NodeView.DEFAULT) instanceof SPIRVStamp)) {
            loadStamp = loadStamp(loadIndexed.stamp(NodeView.DEFAULT), elementKind, false);
        }
        address = createArrayAccess(graph, loadIndexed, elementKind);
        ReadNode memoryRead = graph.add(new ReadNode(address, NamedLocationIdentity.getArrayLocation(elementKind), loadStamp, OnHeapMemoryAccess.BarrierType.NONE));
        loadIndexed.replaceAtUsages(memoryRead);
        graph.replaceFixed(loadIndexed, memoryRead);
    }

    private boolean isPrivateIdNode(StoreIndexedNode storeIndexed) {
        Node nd = storeIndexed.inputs().first().asNode();
        return (nd instanceof FixedArrayNode);
    }

    private boolean isLocalIdNode(StoreIndexedNode storeIndexed) {
        // Either the node has as input a LocalArray or has a node which will be lowered
        // to a LocalArray
        Node nd = storeIndexed.inputs().first().asNode();
        InvokeNode node = nd.inputs().filter(InvokeNode.class).first();
        boolean willLowerToLocalArrayNode = node != null && "Direct#NewArrayNode.newArray".equals(node.callTarget().targetName()) && gpuSnippet;
        return (nd instanceof MarkLocalArray || willLowerToLocalArrayNode);
    }

    private AddressNode createArrayLocalAddress(StructuredGraph graph, ValueNode array, ValueNode index) {
        return graph.unique(new OffsetAddressNode(array, index));
    }

    private AbstractWriteNode createMemWriteNode(JavaKind elementKind, ValueNode value, ValueNode array, AddressNode address, StructuredGraph graph, StoreIndexedNode storeIndexed) {
        AbstractWriteNode memoryWrite;
        if (isLocalIdNode(storeIndexed) || isPrivateIdNode(storeIndexed)) {
            address = createArrayLocalAddress(graph, array, storeIndexed.index());
        }
        ValueNode storeConvertValue = value;
        Stamp valueStamp = value.stamp(NodeView.DEFAULT);
        if (!(valueStamp instanceof SPIRVStamp) || !((SPIRVStamp) valueStamp).getSPIRVKind().isVector()) {
            storeConvertValue = implicitStoreConvert(graph, elementKind, value);
        }
        memoryWrite = graph.add(new WriteNode(address, NamedLocationIdentity.getArrayLocation(elementKind), storeConvertValue, OnHeapMemoryAccess.BarrierType.NONE));
        return memoryWrite;
    }

    @Override
    public void lowerStoreIndexedNode(StoreIndexedNode storeIndexed, LoweringTool tool) {
        StructuredGraph graph = storeIndexed.graph();
        JavaKind elementKind = storeIndexed.elementKind();
        ValueNode valueToStore = storeIndexed.value();
        ValueNode array = storeIndexed.array();
        ValueNode index = storeIndexed.index();
        AddressNode address = createArrayAddress(graph, array, elementKind, index);
        AbstractWriteNode memoryWrite = createMemWriteNode(elementKind, valueToStore, array, address, graph, storeIndexed);
        memoryWrite.setStateAfter(storeIndexed.stateAfter());
        graph.replaceFixedWithFixed(storeIndexed, memoryWrite);
    }

    @Override
    protected void lowerLoadFieldNode(LoadFieldNode loadField, LoweringTool tool) {
        assert loadField.getStackKind() != JavaKind.Illegal;
        StructuredGraph graph = loadField.graph();
        ResolvedJavaField field = loadField.field();
        ValueNode object = loadField.isStatic() ? staticFieldBase(graph, field) : loadField.object();
        Stamp loadStamp = loadStamp(loadField.stamp(NodeView.DEFAULT), field.getJavaKind());
        AddressNode address = createFieldAddress(graph, object, field);
        assert address != null : "Field that is loaded must not be eliminated: " + field.getDeclaringClass().toJavaName(true) + "." + field.getName();
        ReadNode memoryRead = graph.add(new ReadNode(address, fieldLocationIdentity(field), loadStamp, OnHeapMemoryAccess.BarrierType.NONE));
        loadField.replaceAtUsages(memoryRead);
        graph.replaceFixed(loadField, memoryRead);
    }

    @Override
    protected void lowerStoreFieldNode(StoreFieldNode storeField, LoweringTool tool) {
        StructuredGraph graph = storeField.graph();
        ResolvedJavaField field = storeField.field();
        ValueNode object = storeField.isStatic() ? staticFieldBase(graph, field) : storeField.object();
        AddressNode address = createFieldAddress(graph, object, field);
        assert address != null;
        WriteNode memoryWrite = graph.add(new WriteNode(address, fieldLocationIdentity(field), storeField.value(), OnHeapMemoryAccess.BarrierType.NONE));
        memoryWrite.setStateAfter(storeField.stateAfter());
        graph.replaceFixed(storeField, memoryWrite);
    }
}
