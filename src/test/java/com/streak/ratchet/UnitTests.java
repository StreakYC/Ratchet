package com.streak.ratchet;

import com.google.cloud.spanner.Struct;
import com.streak.ratchet.entities.*;
import com.streak.ratchet.schema.EntityTable;
import org.junit.Assert;
import org.junit.Test;


public class UnitTests {
    @Test
    public void annotationReadingKeyFields() throws IllegalAccessException {
        Metadata simple_metadata = new Metadata(Simple.class);
        Assert.assertEquals(simple_metadata.getTableName(), "Simple");

        Metadata inherited_metadata = new Metadata(Inherited.class);
        Assert.assertEquals(inherited_metadata.getTableName(), "Inherited");
    }

    @Test
    public void createFromStruct() throws Throwable {
        Metadata simpleMetadata = new Metadata(Simple.class);
        Metadata inheritedMetadata = new Metadata(Inherited.class);
        InstanceFactory simpleFactory = new InstanceFactory(simpleMetadata);
        InstanceFactory inheritedFactory = new InstanceFactory(inheritedMetadata);

        Struct struct = Struct.newBuilder()
                .set("idString").to("my id")
                .set("idNumber").to(123L)
                .build();
        Simple s = simpleFactory.constructFromStruct(struct);
        Assert.assertEquals(Long.valueOf(123), s.idNumber);
        Assert.assertEquals("my id", s.getIdString());


        Struct inherited_struct = Struct.newBuilder()
                .set("idString").to("my id")
                .set("idNumber").to(123L)
                .set("version").to(1L)
                .build();

        Inherited i = inheritedFactory.constructFromStruct(inherited_struct);
        Assert.assertEquals(Long.valueOf(123), i.idNumber);
        Assert.assertEquals("my id", i.getIdString());
        Assert.assertEquals(Long.valueOf(1), i.getVersion());
    }

    @Test
    public void testTables() throws IllegalAccessException {
        System.out.println("\n---\n");
        System.out.println(String.join("\n\n", new Metadata(NestedList.class).getTable().ddl()));

        Metadata simpleMetadata = new Metadata(Simple.class);
        EntityTable simpleTable = new EntityTable(simpleMetadata);
        System.out.println(String.join("\n\n", simpleTable.ddl()));

        System.out.println("\n---\n");

        Metadata complexMetadata = new Metadata(Complex.class);
        EntityTable complexTable = new EntityTable(complexMetadata);
        System.out.println(String.join("\n\n", complexTable.ddl()));
        System.out.println("\n+++\n");
        System.out.println(String.join("\n\n", complexTable.selectClause()));

        System.out.println("\n---\n");
        Metadata doubleMetadata = new Metadata(DoubleNested.class);
        EntityTable doubleTable = new EntityTable(doubleMetadata);
        System.out.println(String.join("\n\n", doubleTable.ddl()));

    }
}
