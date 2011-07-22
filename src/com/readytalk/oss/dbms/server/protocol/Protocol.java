package com.readytalk.oss.dbms.server.protocol;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.Table;
import com.readytalk.oss.dbms.server.EpidemicServer;
import com.readytalk.oss.dbms.server.StreamUtil;

public class Protocol {
  
  private static final int ClassDefinition = 6;
  private static final int ClassReference = 7;
  private static final int Reference = 8;

  private static volatile Map<Class, Serializer> serializers = new HashMap();
  private static volatile Map<Class, Deserializer> deserializers
    = new HashMap();

  static {
    serializers.put(Integer.class, new Serializer() {
      public void writeTo(WriteContext context, Object v) throws IOException {
        writeInteger(context.out, (Integer) v);
      }
    });

    deserializers.put(Integer.class, new Deserializer() {
      public Object readFrom(ReadContext context, Class c) throws IOException {
        return readInteger(context.in);
      }
    });

    serializers.put(String.class, new Serializer() {
      public void writeTo(WriteContext context, Object v) throws IOException {
        writeString(context.out, (String) v);
      }
    });

    deserializers.put(String.class, new Deserializer() {
      public Object readFrom(ReadContext context, Class c) throws IOException {
        return readString(context.in);
      }
    });

    serializers.put(Class.class, new Serializer() {
      public void writeTo(WriteContext context, Object v) throws IOException {
        write(context, ((Class) v).getName());
      }
    });

    deserializers.put(Class.class, new Deserializer() {
      public Object readFrom(ReadContext context, Class c) throws IOException {
        try {
          return Class.forName((String) read(context));
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    });

    serializers.put(Writable.class, new Serializer() {
      public void writeTo(WriteContext context, Object v) throws IOException {
        ((Writable) v).writeTo(context.server, context.out);
      }
    });

    deserializers.put(Readable.class, new Deserializer() {
      public Object readFrom(ReadContext context, Class c) throws IOException {
        Readable v;
        try {
          v = (Readable) c.newInstance();
        } catch (InstantiationException e) {
          throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
        v.readFrom(context.server, context.in);
        return v;
      }
    });

    serializers.put(Table.class, new Serializer() {
      public void writeTo(WriteContext context, Object v) throws IOException {
        Table t = (Table) v;
        write(context, t.id);
        List<Column> columns = t.primaryKey.columns;
        writeInteger(context.out, columns.size());
        for (Column c: columns) {
          write(context, c);
        }
      }
    });

    deserializers.put(Table.class, new Deserializer() {
      public Object readFrom(ReadContext context, Class c) throws IOException {
        String id = (String) read(context);
        int columnCount = readInteger(context.in);
        List<Column> columns = new ArrayList(columnCount);
        for (int i = 0; i < columnCount; ++i) {
          columns.add((Column) read(context));
        }
        return new Table(columns, id);
      }
    });

    serializers.put(Column.class, new Serializer() {
      public void writeTo(WriteContext context, Object v) throws IOException {
        Column c = (Column) v;
        write(context, c.type);
        write(context, c.id);
      }
    });

    deserializers.put(Column.class, new Deserializer() {
      public Object readFrom(ReadContext context, Class c) throws IOException {
        return new Column((Class) read(context), (String) read(context));
      }
    });
  }

  private static Serializer findSerializer(Class class_) {
    Class c = find(class_, serializers);
    Serializer s = serializers.get(c);
    if (c != class_) {
      synchronized (EpidemicServer.class) {
        Map<Class, Serializer> map = new HashMap(serializers);
        map.put(class_, s);
        serializers = map;
      }
    }
    return s;
  }

  private static Deserializer findDeserializer(Class class_) {
    Class c = find(class_, deserializers);
    Deserializer d = deserializers.get(c);
    if (c != class_) {
      synchronized (EpidemicServer.class) {
        Map<Class, Deserializer> map = new HashMap(deserializers);
        map.put(class_, d);
        deserializers = map;
      }
    }
    return d;
  }

  public static void write(WriteContext context, Object value)
    throws IOException
  {
    Integer id = context.objectIDs.get(value);
    if (id == null) {
      Class c = value.getClass();
      Integer classID = context.classIDs.get(c);
      if (classID == null) {
        int newClassID = context.nextID++;

        context.out.write(ClassDefinition);
        writeInteger(context.out, newClassID);
        writeString(context.out, c.getName());

        context.classIDs.put(c, newClassID);
      } else {
        context.out.write(ClassReference);
        writeInteger(context.out, classID);
      }

      int newID = context.nextID++;
      writeInteger(context.out, newID);
      writeObject(context, value);

      context.objectIDs.put(value, newID);
    } else {
      context.out.write(Reference);
      writeInteger(context.out, id);
    }
  }

  private static void writeInteger(OutputStream out, int v)
    throws IOException
  {
    if (v == (v & 0x7F)) {
      out.write(v);
    } else {
      out.write((v & 0x7F) | 0x80);
      writeInteger(out, v >>> 7);
    }
  }

  private static void writeString(OutputStream out, String s)
    throws IOException
  {
    byte[] bytes = s.getBytes("UTF-8");
    writeInteger(out, bytes.length);
    out.write(bytes);
  }

  private static Class find(Class class_, Map<Class, ?> map) {
    for (Class c = class_; c != Object.class; c = c.getSuperclass()) {
      if (map.containsKey(c)) {
        return c;
      }
    }

    for (Class c: class_.getInterfaces()) {
      if (map.containsKey(c)) {
        return c;
      }
    }

    throw new RuntimeException("no value found for " + class_);
  }

  private static void writeObject(WriteContext context, Object v)
    throws IOException
  {
    findSerializer(v.getClass()).writeTo(context, v);
  }

  public static int readInteger(InputStream in)
    throws IOException
  {
    int b = in.read();
    if (b < 0) {
      throw new EOFException();
    } else if ((b & 0x80) == 0) {
      return b;
    } else {
      return (b & 0x7F) | (readInteger(in) << 7);
    }
  }

  private static String readString(InputStream in)
    throws IOException
  {
    byte[] array = new byte[readInteger(in)];
    if (StreamUtil.readFully(in, array, 0, array.length) != array.length) {
      throw new EOFException();
    }
    return new String(array, "UTF-8");
  }

  public static Object readObject(Class c, ReadContext context)
    throws IOException
  {
    return findDeserializer(c).readFrom(context, c);
  }

  public static Object read(ReadContext context)
    throws IOException
  {
    InputStream in = context.in;
    int flag = in.read();
    switch (flag) {
    case ClassDefinition: {
      int classID = readInteger(in);
      Class c;
      try {
        c = Class.forName(readString(in));
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
      context.classes.put(classID, c);
      return readDefinition(c, context);
    }
      
    case ClassReference: {
      int id = readInteger(in);
      Class value = context.classes.get(id);
      if (value == null) {
        throw new NullPointerException();
      }
      return readDefinition(value, context);
    }
      
    case Reference: {
      int id = readInteger(in);
      Object value = context.objects.get(id);
      if (value == null) {
        throw new NullPointerException();
      }
      return value;
    }
      
    default:
      throw new RuntimeException("unexpected flag: " + flag);
    }    
  }

  public static Object readDefinition(Class c,
                                      ReadContext context)
    throws IOException
  {
    int id = Protocol.readInteger(context.in);
    Object value = readObject(c, context);
    context.objects.put(id, value);
    return value;
  }
}
