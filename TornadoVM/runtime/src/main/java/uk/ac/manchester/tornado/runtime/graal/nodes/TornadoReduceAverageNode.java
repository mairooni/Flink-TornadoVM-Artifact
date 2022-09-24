package uk.ac.manchester.tornado.runtime.graal.nodes;

import jdk.vm.ci.meta.JavaKind;
import org.graalvm.compiler.core.common.type.StampFactory;
import org.graalvm.compiler.graph.Node;
import org.graalvm.compiler.graph.NodeClass;
import org.graalvm.compiler.nodeinfo.NodeInfo;
import org.graalvm.compiler.nodes.ValueNode;
import org.graalvm.compiler.nodes.extended.UnboxNode;
import org.graalvm.compiler.nodes.spi.CanonicalizerTool;

@NodeInfo(shortName = "REDUCE(AVG)")
public class TornadoReduceAverageNode extends ValueNode {
    public static final NodeClass<TornadoReduceAverageNode> TYPE = NodeClass.create(TornadoReduceAverageNode.class);
    @Input
    Node node1;
    @Input
    Node node2;
    @Input
    Node node3;
    @Input
    Node node4;
    @Input
    Node node5;
    @Input
    Node node6;
    @Input
    Node accum;

    public TornadoReduceAverageNode(Node node1, Node node2, Node node3, Node node4, Node node5, Node node6, Node accum) {
        super(TYPE, StampFactory.forKind(JavaKind.Double));
        this.node1 = node1;
        this.node2 = node2;
        this.node3 = node3;
        this.node4 = node4;
        this.node5 = node5;
        this.node6 = node6;
        this.accum = accum;
    }

    public Node getAccum() {
        return this.accum;
    }

}
