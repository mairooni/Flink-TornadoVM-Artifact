package uk.ac.manchester.tornado.api.flink;

import java.util.ArrayList;
import java.util.HashMap;

public class FlinkCompilerInfo {

    // ----- TornadoTupleReplacement
    private boolean hasTuples;
    private int tupleSize;
    private int tupleSizeSecondDataSet;
    private ArrayList<Class> tupleFieldKind;
    private ArrayList<Class> tupleFieldKindSecondDataSet;
    private Class storeJavaKind;
    private int returnTupleSize;
    private boolean returnTuple;
    private ArrayList<Class> returnFieldKind;
    private boolean nestedTuples;
    private int nestedTupleField;
    private int sizeOfNestedTuple;
    private boolean arrayField;
    private int tupleArrayFieldNo;
    private boolean broadcastedArrayField;
    private int broadcastedTupleArrayFieldNo;
    private boolean returnArrayField;
    private int returnTupleArrayFieldNo;
    // ----- TornadoTupleOffset
    private boolean differentTypes = false;
    private ArrayList<Integer> fieldSizes = new ArrayList<>();
    private ArrayList<String> fieldTypes = new ArrayList<>();
    private int arrayFieldTotalBytes;
    private int returnArrayFieldTotalBytes;
    private int broadcastedArrayFieldTotalBytes;
    private String arrayType;
    private int broadcastedSize;
    // for KMeans
    private boolean differentTypesInner = false;
    private ArrayList<Integer> fieldSizesInner = new ArrayList<>();
    private ArrayList<String> fieldTypesInner = new ArrayList<>();
    // --
    private boolean differentTypesRet = false;
    private ArrayList<Integer> fieldSizesRet = new ArrayList<>();
    private ArrayList<String> fieldTypesRet = new ArrayList<>();
    private ArrayList<String> fieldTypesRetCopy = new ArrayList<>();
    // ----- TornadoCollectionElimination
    private boolean broadcastedDataset;
    private String collectionName;
    private int broadCollectionSize;
    // ----- TornadoMatrixFlattening
    private boolean isMatrix;
    private int rowSizeMatrix1;
    private int columnSizeMatrix1;
    private int columnSizeMatrix2;
    private int rowSizeMatrix2;
    private Class matrixType;
    private int matrixTypeSize;
    // flink info
    private int arraySize;
    // ----- TornadoMatrixOffset
    private HashMap<Object, Object> readOuterIndex = new HashMap<>();
    private HashMap<Object, Object> writeOuterIndex = new HashMap<>();

    // setters
    // --- TornadoTupleReplacement
    public void setHasTuples(boolean hasTuples) {
        this.hasTuples = hasTuples;
    }

    public void setTupleSize(int tupleSize) {
        this.tupleSize = tupleSize;
    }

    public void setTupleSizeSecondDataSet(int tupleSizeSecondDataSet) {
        this.tupleSizeSecondDataSet = tupleSizeSecondDataSet;
    }

    public void setTupleFieldKind(ArrayList<Class> tupleFieldKind) {
        this.tupleFieldKind = tupleFieldKind;
    }

    public void setTupleFieldKindSecondDataSet(ArrayList<Class> tupleFieldKindSecondDataSet) {
        this.tupleFieldKindSecondDataSet = tupleFieldKindSecondDataSet;
    }

    public void setStoreJavaKind(Class storeJavaKind) {
        this.storeJavaKind = storeJavaKind;
    }

    public void setReturnTupleSize(int returnTupleSize) {
        this.returnTupleSize = returnTupleSize;
    }

    public void setReturnTuple(boolean returnTuple) {
        this.returnTuple = returnTuple;
    }

    public void setReturnFieldKind(ArrayList<Class> returnFieldKind) {
        this.returnFieldKind = returnFieldKind;
    }

    public void setNestedTuples(boolean nestedTuples) {
        this.nestedTuples = nestedTuples;
    }

    public void setNestedTupleField(int nestedTupleField) {
        this.nestedTupleField = nestedTupleField;
    }

    public void setSizeOfNestedTuple(int sizeOfNestedTuple) {
        this.sizeOfNestedTuple = sizeOfNestedTuple;
    }

    public void setArrayField(boolean arrayField) {
        this.arrayField = arrayField;
    }

    public void setTupleArrayFieldNo(int tupleArrayFieldNo) {
        this.tupleArrayFieldNo = tupleArrayFieldNo;
    }

    public void setBroadcastedArrayField(boolean broadcastedArrayField) {
        this.broadcastedArrayField = broadcastedArrayField;
    }

    public void setBroadcastedTupleArrayFieldNo(int broadcastedTupleArrayFieldNo) {
        this.broadcastedTupleArrayFieldNo = broadcastedTupleArrayFieldNo;
    }

    public void setReturnArrayField(boolean returnArrayField) {
        this.returnArrayField = returnArrayField;
    }

    public void setReturnTupleArrayFieldNo(int returnTupleArrayFieldNo) {
        this.returnTupleArrayFieldNo = returnTupleArrayFieldNo;
    }

    // --- TornadoTupleOffset
    public void setDifferentTypes(boolean differentTypes) {
        this.differentTypes = differentTypes;
    }

    public void setFieldSizes(ArrayList<Integer> fieldSizes) {
        this.fieldSizes = fieldSizes;
    }

    public void setFieldTypes(ArrayList<String> fieldTypes) {
        this.fieldTypes = fieldTypes;
    }

    public void setDifferentTypesInner(boolean differentTypesInner) {
        this.differentTypesInner = differentTypesInner;
    }

    public void setFieldSizesInner(ArrayList<Integer> fieldSizesInner) {
        this.fieldSizesInner = fieldSizesInner;
    }

    public void setFieldTypesInner(ArrayList<String> fieldTypesInner) {
        this.fieldTypesInner = fieldTypesInner;
    }

    public void setDifferentTypesRet(boolean differentTypesRet) {
        this.differentTypesRet = differentTypesRet;
    }

    public void setFieldSizesRet(ArrayList<Integer> fieldSizesRet) {
        this.fieldSizesRet = fieldSizesRet;
    }

    public void setFieldTypesRet(ArrayList<String> fieldTypesRet) {
        this.fieldTypesRet = fieldTypesRet;
    }

    public void setFieldTypesRetCopy(ArrayList<String> fieldTypesRetCopy) {
        this.fieldTypesRetCopy = fieldTypesRetCopy;
    }

    public void setBroadcastedArrayFieldTotalBytes(int broadcastedArrayFieldTotalBytes) {
        this.broadcastedArrayFieldTotalBytes = broadcastedArrayFieldTotalBytes;
    }

    public void setArrayFieldTotalBytes(int arrayFieldTotalBytes) {
        this.arrayFieldTotalBytes = arrayFieldTotalBytes;
    }

    public void setReturnArrayFieldTotalBytes(int returnArrayFieldTotalBytes) {
        this.returnArrayFieldTotalBytes = returnArrayFieldTotalBytes;
    }

    public void setArrayType(String arrayType) {
        this.arrayType = arrayType;
    }

    public void setBroadcastedSize(int broadcastedSize) {
        this.broadcastedSize = broadcastedSize;
    }

    // TornadoCollectionElimination
    public void setBroadcastedDataset(boolean broadcastedDataset) {
        this.broadcastedDataset = broadcastedDataset;
    }

    // flink
    public void setArraySize(int arraySize) {
        this.arraySize = arraySize;
    }

    // getters
    // --- TornadoTupleReplacement
    public boolean getHasTuples() {
        return this.hasTuples;
    }

    public int getTupleSize() {
        return this.tupleSize;
    }

    public int getTupleSizeSecondDataSet() {
        return this.tupleSizeSecondDataSet;
    }

    public ArrayList<Class> getTupleFieldKind() {
        return this.tupleFieldKind;
    }

    public ArrayList<Class> getTupleFieldKindSecondDataSet() {
        return this.tupleFieldKindSecondDataSet;
    }

    public Class getStoreJavaKind() {
        return this.storeJavaKind;
    }

    public int getReturnTupleSize() {
        return this.returnTupleSize;
    }

    public boolean getReturnTuple() {
        return this.returnTuple;
    }

    public ArrayList<Class> getReturnFieldKind() {
        return this.returnFieldKind;
    }

    public boolean getNestedTuples() {
        return this.nestedTuples;
    }

    public int getNestedTupleField() {
        return this.nestedTupleField;
    }

    public int getSizeOfNestedTuple() {
        return this.sizeOfNestedTuple;
    }

    public boolean getArrayField() {
        return this.arrayField;
    }

    public int getTupleArrayFieldNo() {
        return this.tupleArrayFieldNo;
    }

    public boolean getBroadcastedArrayField() {
        return this.broadcastedArrayField;
    }

    public int getBroadcastedTupleArrayFieldNo() {
        return this.broadcastedTupleArrayFieldNo;
    }

    public boolean getReturnArrayField() {
        return this.returnArrayField;
    }

    public int getReturnTupleArrayFieldNo() {
        return this.returnTupleArrayFieldNo;
    }

    // --- TornadoTupleOffset
    public boolean getDifferentTypes() {
        return this.differentTypes;
    }

    public ArrayList<Integer> getFieldSizes() {
        return this.fieldSizes;
    }

    public ArrayList<String> getFieldTypes() {
        return this.fieldTypes;
    }

    public boolean getDifferentTypesInner() {
        return this.differentTypesInner;
    }

    public ArrayList<Integer> getFieldSizesInner() {
        return this.fieldSizesInner;
    }

    public ArrayList<String> getFieldTypesInner() {
        return this.fieldTypesInner;
    }

    public boolean getDifferentTypesRet() {
        return this.differentTypesRet;
    }

    public ArrayList<Integer> getFieldSizesRet() {
        return this.fieldSizesRet;
    }

    public ArrayList<String> getFieldTypesRet() {
        return this.fieldTypesRet;
    }

    public ArrayList<String> getFieldTypesRetCopy() {
        return this.fieldTypesRetCopy;
    }

    public int getArrayFieldTotalBytes() {
        return this.arrayFieldTotalBytes;
    }

    public int getBroadcastedArrayFieldTotalBytes() {
        return this.broadcastedArrayFieldTotalBytes;
    }

    public int getReturnArrayFieldTotalBytes() {
        return this.returnArrayFieldTotalBytes;
    }

    public int getBroadcastedSize() {
        return this.broadcastedSize;
    }

    // TornadoCollectionElimination
    public boolean getBroadcastedDataset() {
        return this.broadcastedDataset;
    }

    public String getArrayType() {
        return this.arrayType;
    }

    public int getArraySize() {
        return this.arraySize;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public void setBroadCollectionSize(int broadCollectionSize) {
        this.broadCollectionSize = broadCollectionSize;
    }

    public int getBroadCollectionSize() {
        return this.broadCollectionSize;
    }

    public String getCollectionName() {
        return this.collectionName;
    }

    // TornadoMatrixFlattening

    public void setIsMatrix() {
        this.isMatrix = true;
    }

    public boolean getIsMatrix() {
        return isMatrix;
    }

    public void setRowSizeMatrix1(int rowSizeMatrix1) {
        this.rowSizeMatrix1 = rowSizeMatrix1;
    }

    public void setColumnSizeMatrix1(int columnSizeMatrix1) {
        this.columnSizeMatrix1 = columnSizeMatrix1;
    }

    public int getColumnSizeMatrix1() {
        return this.columnSizeMatrix1;
    }

    public int getRowSizeMatrix1() {
        return this.rowSizeMatrix1;
    }

    public void setColumnSizeMatrix2(int columnSizeMatrix2) {
        this.columnSizeMatrix2 = columnSizeMatrix2;
    }

    public int getColumnSizeMatrix2() {
        return this.columnSizeMatrix2;
    }

    public void setRowSizeMatrix2(int rowSizeMatrix2) {
        this.rowSizeMatrix2 = rowSizeMatrix2;
    }

    public int getRowSizeMatrix2() {
        return this.rowSizeMatrix2;
    }

    public void setMatrixType(Class matrixType) {
        this.matrixType = matrixType;
    }

    public Class getMatrixType() {
        return this.matrixType;
    }

    public void setMatrixTypeSize(int matrixTypeSize) {
        this.matrixTypeSize = matrixTypeSize;
    }

    public int getMatrixTypeSize() {
        return this.matrixTypeSize;
    }

    // TornadoMatrixOffset
    public void setReadOuterIndex(HashMap readOuterIndex) {
        this.readOuterIndex = readOuterIndex;
    }

    public HashMap getReadOuterIndex() {
        return this.readOuterIndex;
    }

    public void setWriteOuterIndex(HashMap writeOuterIndex) {
        this.writeOuterIndex = writeOuterIndex;
    }

    public HashMap getWriteOuterIndex() {
        return this.writeOuterIndex;
    }

}
