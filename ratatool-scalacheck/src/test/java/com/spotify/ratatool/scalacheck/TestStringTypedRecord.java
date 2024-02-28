/**
 * Generated ad-hoc using sbt-avro `avroStringType := String` setting.
 */
package com.spotify.ratatool.scalacheck;

import org.apache.avro.specific.SpecificData;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class TestStringTypedRecord extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
    private static final long serialVersionUID = 1773563257129780214L;
    public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"TestStringTypedRecord\",\"namespace\":\"com.spotify.ratatool.scalacheck\",\"fields\":[{\"name\":\"string_field\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"nullable_string_field\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"array_field\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}},{\"name\":\"map_field\",\"type\":[{\"type\":\"map\",\"values\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"avro.java.string\":\"String\"}]}]}");
    public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

    private static SpecificData MODEL$ = new SpecificData();

    private static final BinaryMessageEncoder<TestStringTypedRecord> ENCODER =
            new BinaryMessageEncoder<TestStringTypedRecord>(MODEL$, SCHEMA$);

    private static final BinaryMessageDecoder<TestStringTypedRecord> DECODER =
            new BinaryMessageDecoder<TestStringTypedRecord>(MODEL$, SCHEMA$);

    /**
     * Return the BinaryMessageDecoder instance used by this class.
     */
    public static BinaryMessageDecoder<TestStringTypedRecord> getDecoder() {
        return DECODER;
    }

    /**
     * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
     * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
     */
    public static BinaryMessageDecoder<TestStringTypedRecord> createDecoder(SchemaStore resolver) {
        return new BinaryMessageDecoder<TestStringTypedRecord>(MODEL$, SCHEMA$, resolver);
    }

    /** Serializes this TestStringTypedRecord to a ByteBuffer. */
    public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
        return ENCODER.encode(this);
    }

    /** Deserializes a TestStringTypedRecord from a ByteBuffer. */
    public static TestStringTypedRecord fromByteBuffer(
            java.nio.ByteBuffer b) throws java.io.IOException {
        return DECODER.decode(b);
    }

    public java.lang.String string_field;
    public java.lang.String nullable_string_field;
    public java.util.List<java.lang.String> array_field;
    public java.lang.Object map_field;

    /**
     * Default constructor.  Note that this does not initialize fields
     * to their default values from the schema.  If that is desired then
     * one should use <code>newBuilder()</code>.
     */
    public TestStringTypedRecord() {}

    /**
     * All-args constructor.
     * @param string_field The new value for string_field
     * @param nullable_string_field The new value for nullable_string_field
     * @param array_field The new value for array_field
     * @param map_field The new value for map_field
     */
    public TestStringTypedRecord(java.lang.String string_field, java.lang.String nullable_string_field, java.util.List<java.lang.String> array_field, java.lang.Object map_field) {
        this.string_field = string_field;
        this.nullable_string_field = nullable_string_field;
        this.array_field = array_field;
        this.map_field = map_field;
    }

    public org.apache.avro.Schema getSchema() { return SCHEMA$; }
    // Used by DatumWriter.  Applications should not call.
    public java.lang.Object get(int field$) {
        switch (field$) {
            case 0: return string_field;
            case 1: return nullable_string_field;
            case 2: return array_field;
            case 3: return map_field;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    // Used by DatumReader.  Applications should not call.
    @SuppressWarnings(value="unchecked")
    public void put(int field$, java.lang.Object value$) {
        switch (field$) {
            case 0: string_field = (java.lang.String)value$; break;
            case 1: nullable_string_field = (java.lang.String)value$; break;
            case 2: array_field = (java.util.List<java.lang.String>)value$; break;
            case 3: map_field = (java.lang.Object)value$; break;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    /**
     * Gets the value of the 'string_field' field.
     * @return The value of the 'string_field' field.
     */
    public java.lang.String getStringField() {
        return string_field;
    }

    /**
     * Sets the value of the 'string_field' field.
     * @param value the value to set.
     */
    public void setStringField(java.lang.String value) {
        this.string_field = value;
    }

    /**
     * Gets the value of the 'nullable_string_field' field.
     * @return The value of the 'nullable_string_field' field.
     */
    public java.lang.String getNullableStringField() {
        return nullable_string_field;
    }

    /**
     * Sets the value of the 'nullable_string_field' field.
     * @param value the value to set.
     */
    public void setNullableStringField(java.lang.String value) {
        this.nullable_string_field = value;
    }

    /**
     * Gets the value of the 'array_field' field.
     * @return The value of the 'array_field' field.
     */
    public java.util.List<java.lang.String> getArrayField() {
        return array_field;
    }

    /**
     * Sets the value of the 'array_field' field.
     * @param value the value to set.
     */
    public void setArrayField(java.util.List<java.lang.String> value) {
        this.array_field = value;
    }

    /**
     * Gets the value of the 'map_field' field.
     * @return The value of the 'map_field' field.
     */
    public java.lang.Object getMapField() {
        return map_field;
    }

    /**
     * Sets the value of the 'map_field' field.
     * @param value the value to set.
     */
    public void setMapField(java.lang.Object value) {
        this.map_field = value;
    }

    /**
     * Creates a new TestStringTypedRecord RecordBuilder.
     * @return A new TestStringTypedRecord RecordBuilder
     */
    public static com.spotify.ratatool.scalacheck.TestStringTypedRecord.Builder newBuilder() {
        return new com.spotify.ratatool.scalacheck.TestStringTypedRecord.Builder();
    }

    /**
     * Creates a new TestStringTypedRecord RecordBuilder by copying an existing Builder.
     * @param other The existing builder to copy.
     * @return A new TestStringTypedRecord RecordBuilder
     */
    public static com.spotify.ratatool.scalacheck.TestStringTypedRecord.Builder newBuilder(com.spotify.ratatool.scalacheck.TestStringTypedRecord.Builder other) {
        return new com.spotify.ratatool.scalacheck.TestStringTypedRecord.Builder(other);
    }

    /**
     * Creates a new TestStringTypedRecord RecordBuilder by copying an existing TestStringTypedRecord instance.
     * @param other The existing instance to copy.
     * @return A new TestStringTypedRecord RecordBuilder
     */
    public static com.spotify.ratatool.scalacheck.TestStringTypedRecord.Builder newBuilder(com.spotify.ratatool.scalacheck.TestStringTypedRecord other) {
        return new com.spotify.ratatool.scalacheck.TestStringTypedRecord.Builder(other);
    }

    /**
     * RecordBuilder for TestStringTypedRecord instances.
     */
    public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<TestStringTypedRecord>
            implements org.apache.avro.data.RecordBuilder<TestStringTypedRecord> {

        private java.lang.String string_field;
        private java.lang.String nullable_string_field;
        private java.util.List<java.lang.String> array_field;
        private java.lang.Object map_field;

        /** Creates a new Builder */
        private Builder() {
            super(SCHEMA$);
        }

        /**
         * Creates a Builder by copying an existing Builder.
         * @param other The existing Builder to copy.
         */
        private Builder(com.spotify.ratatool.scalacheck.TestStringTypedRecord.Builder other) {
            super(other);
            if (isValidValue(fields()[0], other.string_field)) {
                this.string_field = data().deepCopy(fields()[0].schema(), other.string_field);
                fieldSetFlags()[0] = true;
            }
            if (isValidValue(fields()[1], other.nullable_string_field)) {
                this.nullable_string_field = data().deepCopy(fields()[1].schema(), other.nullable_string_field);
                fieldSetFlags()[1] = true;
            }
            if (isValidValue(fields()[2], other.array_field)) {
                this.array_field = data().deepCopy(fields()[2].schema(), other.array_field);
                fieldSetFlags()[2] = true;
            }
            if (isValidValue(fields()[3], other.map_field)) {
                this.map_field = data().deepCopy(fields()[3].schema(), other.map_field);
                fieldSetFlags()[3] = true;
            }
        }

        /**
         * Creates a Builder by copying an existing TestStringTypedRecord instance
         * @param other The existing instance to copy.
         */
        private Builder(com.spotify.ratatool.scalacheck.TestStringTypedRecord other) {
            super(SCHEMA$);
            if (isValidValue(fields()[0], other.string_field)) {
                this.string_field = data().deepCopy(fields()[0].schema(), other.string_field);
                fieldSetFlags()[0] = true;
            }
            if (isValidValue(fields()[1], other.nullable_string_field)) {
                this.nullable_string_field = data().deepCopy(fields()[1].schema(), other.nullable_string_field);
                fieldSetFlags()[1] = true;
            }
            if (isValidValue(fields()[2], other.array_field)) {
                this.array_field = data().deepCopy(fields()[2].schema(), other.array_field);
                fieldSetFlags()[2] = true;
            }
            if (isValidValue(fields()[3], other.map_field)) {
                this.map_field = data().deepCopy(fields()[3].schema(), other.map_field);
                fieldSetFlags()[3] = true;
            }
        }

        /**
         * Gets the value of the 'string_field' field.
         * @return The value.
         */
        public java.lang.String getStringField() {
            return string_field;
        }

        /**
         * Sets the value of the 'string_field' field.
         * @param value The value of 'string_field'.
         * @return This builder.
         */
        public com.spotify.ratatool.scalacheck.TestStringTypedRecord.Builder setStringField(java.lang.String value) {
            validate(fields()[0], value);
            this.string_field = value;
            fieldSetFlags()[0] = true;
            return this;
        }

        /**
         * Checks whether the 'string_field' field has been set.
         * @return True if the 'string_field' field has been set, false otherwise.
         */
        public boolean hasStringField() {
            return fieldSetFlags()[0];
        }


        /**
         * Clears the value of the 'string_field' field.
         * @return This builder.
         */
        public com.spotify.ratatool.scalacheck.TestStringTypedRecord.Builder clearStringField() {
            string_field = null;
            fieldSetFlags()[0] = false;
            return this;
        }

        /**
         * Gets the value of the 'nullable_string_field' field.
         * @return The value.
         */
        public java.lang.String getNullableStringField() {
            return nullable_string_field;
        }

        /**
         * Sets the value of the 'nullable_string_field' field.
         * @param value The value of 'nullable_string_field'.
         * @return This builder.
         */
        public com.spotify.ratatool.scalacheck.TestStringTypedRecord.Builder setNullableStringField(java.lang.String value) {
            validate(fields()[1], value);
            this.nullable_string_field = value;
            fieldSetFlags()[1] = true;
            return this;
        }

        /**
         * Checks whether the 'nullable_string_field' field has been set.
         * @return True if the 'nullable_string_field' field has been set, false otherwise.
         */
        public boolean hasNullableStringField() {
            return fieldSetFlags()[1];
        }


        /**
         * Clears the value of the 'nullable_string_field' field.
         * @return This builder.
         */
        public com.spotify.ratatool.scalacheck.TestStringTypedRecord.Builder clearNullableStringField() {
            nullable_string_field = null;
            fieldSetFlags()[1] = false;
            return this;
        }

        /**
         * Gets the value of the 'array_field' field.
         * @return The value.
         */
        public java.util.List<java.lang.String> getArrayField() {
            return array_field;
        }

        /**
         * Sets the value of the 'array_field' field.
         * @param value The value of 'array_field'.
         * @return This builder.
         */
        public com.spotify.ratatool.scalacheck.TestStringTypedRecord.Builder setArrayField(java.util.List<java.lang.String> value) {
            validate(fields()[2], value);
            this.array_field = value;
            fieldSetFlags()[2] = true;
            return this;
        }

        /**
         * Checks whether the 'array_field' field has been set.
         * @return True if the 'array_field' field has been set, false otherwise.
         */
        public boolean hasArrayField() {
            return fieldSetFlags()[2];
        }


        /**
         * Clears the value of the 'array_field' field.
         * @return This builder.
         */
        public com.spotify.ratatool.scalacheck.TestStringTypedRecord.Builder clearArrayField() {
            array_field = null;
            fieldSetFlags()[2] = false;
            return this;
        }

        /**
         * Gets the value of the 'map_field' field.
         * @return The value.
         */
        public java.lang.Object getMapField() {
            return map_field;
        }

        /**
         * Sets the value of the 'map_field' field.
         * @param value The value of 'map_field'.
         * @return This builder.
         */
        public com.spotify.ratatool.scalacheck.TestStringTypedRecord.Builder setMapField(java.lang.Object value) {
            validate(fields()[3], value);
            this.map_field = value;
            fieldSetFlags()[3] = true;
            return this;
        }

        /**
         * Checks whether the 'map_field' field has been set.
         * @return True if the 'map_field' field has been set, false otherwise.
         */
        public boolean hasMapField() {
            return fieldSetFlags()[3];
        }


        /**
         * Clears the value of the 'map_field' field.
         * @return This builder.
         */
        public com.spotify.ratatool.scalacheck.TestStringTypedRecord.Builder clearMapField() {
            map_field = null;
            fieldSetFlags()[3] = false;
            return this;
        }

        @Override
        @SuppressWarnings("unchecked")
        public TestStringTypedRecord build() {
            try {
                TestStringTypedRecord record = new TestStringTypedRecord();
                record.string_field = fieldSetFlags()[0] ? this.string_field : (java.lang.String) defaultValue(fields()[0]);
                record.nullable_string_field = fieldSetFlags()[1] ? this.nullable_string_field : (java.lang.String) defaultValue(fields()[1]);
                record.array_field = fieldSetFlags()[2] ? this.array_field : (java.util.List<java.lang.String>) defaultValue(fields()[2]);
                record.map_field = fieldSetFlags()[3] ? this.map_field : (java.lang.Object) defaultValue(fields()[3]);
                return record;
            } catch (java.lang.Exception e) {
                throw new org.apache.avro.AvroRuntimeException(e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static final org.apache.avro.io.DatumWriter<TestStringTypedRecord>
            WRITER$ = (org.apache.avro.io.DatumWriter<TestStringTypedRecord>)MODEL$.createDatumWriter(SCHEMA$);

    @Override public void writeExternal(java.io.ObjectOutput out)
            throws java.io.IOException {
        WRITER$.write(this, SpecificData.getEncoder(out));
    }

    @SuppressWarnings("unchecked")
    private static final org.apache.avro.io.DatumReader<TestStringTypedRecord>
            READER$ = (org.apache.avro.io.DatumReader<TestStringTypedRecord>)MODEL$.createDatumReader(SCHEMA$);

    @Override public void readExternal(java.io.ObjectInput in)
            throws java.io.IOException {
        READER$.read(this, SpecificData.getDecoder(in));
    }

}
