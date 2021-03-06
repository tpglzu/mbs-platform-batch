/**
 * Autogenerated by Thrift Compiler (0.14.2)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.ycu.tang.msbplatform.batch.thrift;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.14.2)", date = "2021-10-17")
public class Data implements org.apache.thrift.TBase<Data, Data._Fields>, java.io.Serializable, Cloneable, Comparable<Data> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("Data");

  private static final org.apache.thrift.protocol.TField PEDIGREE_FIELD_DESC = new org.apache.thrift.protocol.TField("pedigree", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField DATAUNIT_FIELD_DESC = new org.apache.thrift.protocol.TField("dataunit", org.apache.thrift.protocol.TType.STRUCT, (short)2);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new DataStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new DataTupleSchemeFactory();

  public @org.apache.thrift.annotation.Nullable Pedigree pedigree; // required
  public @org.apache.thrift.annotation.Nullable DataUnit dataunit; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    PEDIGREE((short)1, "pedigree"),
    DATAUNIT((short)2, "dataunit");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // PEDIGREE
          return PEDIGREE;
        case 2: // DATAUNIT
          return DATAUNIT;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.PEDIGREE, new org.apache.thrift.meta_data.FieldMetaData("pedigree", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, Pedigree.class)));
    tmpMap.put(_Fields.DATAUNIT, new org.apache.thrift.meta_data.FieldMetaData("dataunit", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, DataUnit.class)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(Data.class, metaDataMap);
  }

  public Data() {
  }

  public Data(
    Pedigree pedigree,
    DataUnit dataunit)
  {
    this();
    this.pedigree = pedigree;
    this.dataunit = dataunit;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Data(Data other) {
    if (other.isSetPedigree()) {
      this.pedigree = new Pedigree(other.pedigree);
    }
    if (other.isSetDataunit()) {
      this.dataunit = new DataUnit(other.dataunit);
    }
  }

  public Data deepCopy() {
    return new Data(this);
  }

  @Override
  public void clear() {
    this.pedigree = null;
    this.dataunit = null;
  }

  @org.apache.thrift.annotation.Nullable
  public Pedigree getPedigree() {
    return this.pedigree;
  }

  public Data setPedigree(@org.apache.thrift.annotation.Nullable Pedigree pedigree) {
    this.pedigree = pedigree;
    return this;
  }

  public void unsetPedigree() {
    this.pedigree = null;
  }

  /** Returns true if field pedigree is set (has been assigned a value) and false otherwise */
  public boolean isSetPedigree() {
    return this.pedigree != null;
  }

  public void setPedigreeIsSet(boolean value) {
    if (!value) {
      this.pedigree = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public DataUnit getDataunit() {
    return this.dataunit;
  }

  public Data setDataunit(@org.apache.thrift.annotation.Nullable DataUnit dataunit) {
    this.dataunit = dataunit;
    return this;
  }

  public void unsetDataunit() {
    this.dataunit = null;
  }

  /** Returns true if field dataunit is set (has been assigned a value) and false otherwise */
  public boolean isSetDataunit() {
    return this.dataunit != null;
  }

  public void setDataunitIsSet(boolean value) {
    if (!value) {
      this.dataunit = null;
    }
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case PEDIGREE:
      if (value == null) {
        unsetPedigree();
      } else {
        setPedigree((Pedigree)value);
      }
      break;

    case DATAUNIT:
      if (value == null) {
        unsetDataunit();
      } else {
        setDataunit((DataUnit)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case PEDIGREE:
      return getPedigree();

    case DATAUNIT:
      return getDataunit();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case PEDIGREE:
      return isSetPedigree();
    case DATAUNIT:
      return isSetDataunit();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof Data)
      return this.equals((Data)that);
    return false;
  }

  public boolean equals(Data that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_pedigree = true && this.isSetPedigree();
    boolean that_present_pedigree = true && that.isSetPedigree();
    if (this_present_pedigree || that_present_pedigree) {
      if (!(this_present_pedigree && that_present_pedigree))
        return false;
      if (!this.pedigree.equals(that.pedigree))
        return false;
    }

    boolean this_present_dataunit = true && this.isSetDataunit();
    boolean that_present_dataunit = true && that.isSetDataunit();
    if (this_present_dataunit || that_present_dataunit) {
      if (!(this_present_dataunit && that_present_dataunit))
        return false;
      if (!this.dataunit.equals(that.dataunit))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetPedigree()) ? 131071 : 524287);
    if (isSetPedigree())
      hashCode = hashCode * 8191 + pedigree.hashCode();

    hashCode = hashCode * 8191 + ((isSetDataunit()) ? 131071 : 524287);
    if (isSetDataunit())
      hashCode = hashCode * 8191 + dataunit.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(Data other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetPedigree(), other.isSetPedigree());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPedigree()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.pedigree, other.pedigree);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetDataunit(), other.isSetDataunit());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDataunit()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.dataunit, other.dataunit);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("Data(");
    boolean first = true;

    sb.append("pedigree:");
    if (this.pedigree == null) {
      sb.append("null");
    } else {
      sb.append(this.pedigree);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("dataunit:");
    if (this.dataunit == null) {
      sb.append("null");
    } else {
      sb.append(this.dataunit);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (pedigree == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'pedigree' was not present! Struct: " + toString());
    }
    if (dataunit == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'dataunit' was not present! Struct: " + toString());
    }
    // check for sub-struct validity
    if (pedigree != null) {
      pedigree.validate();
    }
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class DataStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public DataStandardScheme getScheme() {
      return new DataStandardScheme();
    }
  }

  private static class DataStandardScheme extends org.apache.thrift.scheme.StandardScheme<Data> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, Data struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // PEDIGREE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.pedigree = new Pedigree();
              struct.pedigree.read(iprot);
              struct.setPedigreeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // DATAUNIT
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.dataunit = new DataUnit();
              struct.dataunit.read(iprot);
              struct.setDataunitIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, Data struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.pedigree != null) {
        oprot.writeFieldBegin(PEDIGREE_FIELD_DESC);
        struct.pedigree.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.dataunit != null) {
        oprot.writeFieldBegin(DATAUNIT_FIELD_DESC);
        struct.dataunit.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class DataTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public DataTupleScheme getScheme() {
      return new DataTupleScheme();
    }
  }

  private static class DataTupleScheme extends org.apache.thrift.scheme.TupleScheme<Data> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, Data struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      struct.pedigree.write(oprot);
      struct.dataunit.write(oprot);
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, Data struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      struct.pedigree = new Pedigree();
      struct.pedigree.read(iprot);
      struct.setPedigreeIsSet(true);
      struct.dataunit = new DataUnit();
      struct.dataunit.read(iprot);
      struct.setDataunitIsSet(true);
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

