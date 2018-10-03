# Ratchet

Ratchet is a Java data access API designed for Google Spanner.  It is heavily inspired by Objectify, and both share the
same goal of making it easier to build applications.  Unlike Objectify, Ratchet is opinionated about how you need to lay
out your schema. Ratchet currently only supports Immutable data.  We assume that each row will have not only an id, but
also version and a Boolean tip properties that are automatically managed by Ratchet.  This is inspired by
[Aleem's blog post](https://cloudplatform.googleblog.com/2016/08/building-immutable-entities-into-Google-Cloud-Datastore.html)

When an object is first saved by Ratchet (such as by calling `RatchetService.ratchet().save(object)`), the row's
version field is set to 1 and the tip field is set to true.  When an object is updated (by calling `save(object)`
again), then the current row in Spanner with the object's id and tip=true has the tip field set to null, and a new row
is added to the table with tip=true and version set to the previous tip row's version plus one.

## Basic Usage

```
import com.streak.ratchet.Annotations.Key;
import com.streak.ratchet.Annotations.Table;
import com.streak.ratchet.Annotations.Tip;
import com.streak.ratchet.Annotations.Version;

@Table
public class Simple {
    @Key
    public String idString;

    @Version
    public Long idNumber;
    
    @Tip
    public Boolean tip;
}
```

```
Simple s = new Simple();
s.idString = "key";
RatchetService.save(s);

Simple loaded = RatchetService.load(Simple.class, "key");

```


## Schema

Classes annotated with `@Table` assume a consistent schema (see `SchemaWriter.java` for how to automatically create
these).  As a minimum they require a table and an index.

We'll assume this slightly more complex class:

```
@Table
public class Complex {
    @Key
    public Long id;

    @Version
    public Long version;

    @Tip
    public Boolean tip;

    public static class Subclass {
        public STATUS status;
        public Date thatTime;

        public Subclass(STATUS status) {
            this.status = status;
            thatTime = new Date();
        }
        public Subclass() {}
    }

    public enum STATUS {
        GOOD, BAD
    }

    public List<Subclass> sub;

    public long primitive;
}
```


```
CREATE TABLE Complex (
    id INT64 NOT NULL,
    version INT64 NOT NULL,
    primitive INT64,
    tip BOOL,
) PRIMARY KEY (id, version)

CREATE UNIQUE NULL_FILTERED INDEX ComplexTipConstraint
ON Complex (
    id,
    tip
)
```

The table `Complex` represents our non-nested data.  TipConstraint is used to ensure that only one tip can exist
in the database for each id and to allow us to quickly find which version is tip (since version is part of the primary
key, we don't need to explicitly add it to our STORING declaration).
Lists, such as `sub`, are stored in interleaved tables.

```
CREATE TABLE Complex_sub (
    id INT64 NOT NULL,
    version INT64 NOT NULL,
    offset INT64 NOT NULL,
    sub_status STRING(MAX),
    sub_thatTime TIMESTAMP,
) PRIMARY KEY (id, version, offset),
INTERLEAVE IN PARENT Complex ON DELETE CASCADE
```

In the future, we may support other ways of serializing data into Spanner rows.

## Mapping of Ratchet Objects to other things

### Metadata

Metadata maps to a java class that you want to store in Spanner.  It will either have an @Table attribution, or it will
be class referred to by the main class (see ListClassTranslator)

Metadata concerns itself with the overall structure of the class, and how it relates to our assumptions in Spanner.
We assume that we will have one version field, and Metadata can both fetch that for us, and validate it.

### MetadataField

MetadataField maps to a field that we want to store in Spanner.  It maps to one Metadata, and multiple Selectable
objects (child tables and spanner fields).  Fields returned by select queries are named after MetadataFields, which
greatly simplifies parsing.

### SpannerField

Maps to a concrete field in Spanner

### SpannerTable

Interface for any concrete spanner table.
