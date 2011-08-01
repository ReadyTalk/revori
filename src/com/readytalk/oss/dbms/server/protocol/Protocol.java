package com.readytalk.oss.dbms.server.protocol;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.lang.reflect.InvocationTargetException;

import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.Table;
import com.readytalk.oss.dbms.server.EpidemicServer;
import com.readytalk.oss.dbms.server.StreamUtil;

public class Protocol {
  
  private static final int ClassDefinition = 6;
  private static final int ClassReference = 7;
  private static final int Reference = 8;

  private static volatile Map<Class<?>, Serializer<?>> serializers = new HashMap<Class<?>, Serializer<?>>();
  private static volatile Map<Class<?>, Deserializer<?>> deserializers
    = new HashMap<Class<?>, Deserializer<?>>();

  static {
    serializers.put(Boolean.class, new Serializer<Boolean>() {
      public void writeTo(WriteContext context, Boolean v) throws IOException {
        writeBoolean(context.out, v);
      }
    });

    deserializers.put(Boolean.class, new Deserializer<Boolean>() {
      public Boolean readFrom(ReadContext context, Class<? extends Boolean> c) throws IOException {
        return readBoolean(context.in);
      }
    });

    serializers.put(Integer.class, new Serializer<Integer>() {
      public void writeTo(WriteContext context, Integer v) throws IOException {
        writeInteger(context.out, v);
      }
    });

    deserializers.put(Integer.class, new Deserializer<Integer>() {
      public Integer readFrom(ReadContext context, Class<? extends Integer> c) throws IOException {
        return readInteger(context.in);
      }
    });

    serializers.put(Long.class, new Serializer<Long>() {
      public void writeTo(WriteContext context, Long v) throws IOException {
        writeLong(context.out, v);
      }
    });

    deserializers.put(Long.class, new Deserializer<Long>() {
      public Long readFrom(ReadContext context, Class<? extends Long> c) throws IOException {
        return readLong(context.in);
      }
    });

    serializers.put(String.class, new Serializer<String>() {
      public void writeTo(WriteContext context, String v) throws IOException {
        writeString(context.out, v);
      }
    });

    deserializers.put(String.class, new Deserializer<String>() {
      public String readFrom(ReadContext context, Class<? extends String> c) throws IOException {
        return readString(context.in);
      }
    });

    serializers.put(Class.class, new Serializer<Class<?>>() {
      public void writeTo(WriteContext context, Class<?> v) throws IOException {
        write(context, v.getName());
      }
    });

    deserializers.put(Class.class, new Deserializer<Class<?>>() {
      public Class<?> readFrom(ReadContext context, Class<? extends Class<?>> c) throws IOException {
        try {
          return Class.forName((String) read(context));
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    });

    serializers.put(Writable.class, new Serializer<Writable>() {
      public void writeTo(WriteContext context, Writable v) throws IOException {
        v.writeTo(context.out);
      }
    });

    deserializers.put(Readable.class, new Deserializer<Readable>() {
      public Readable readFrom(ReadContext context, Class<? extends Readable> c) throws IOException {
        Readable v;
        try {
          v = (Readable) c.newInstance();
        } catch (InstantiationException e) {
          throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
        v.readFrom(context.in);
        return v;
      }
    });

    serializers.put(Stringable.class, new Serializer<Stringable>() {
      public void writeTo(WriteContext context, Stringable v)
        throws IOException
      {
        writeString(context.out, v.asString());
      }
    });

    deserializers.put(Stringable.class, new Deserializer<Stringable>() {
      public Stringable readFrom(ReadContext context,
                                 Class<? extends Stringable> c)
        throws IOException
      {
        try {
          return (Stringable) c.getConstructor(String.class).newInstance
            (readString(context.in));
        } catch (NoSuchMethodException e) {
          throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
          throw new RuntimeException(e);
        } catch (InstantiationException e) {
          throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
    });

    serializers.put(Table.class, new Serializer<Table>() {
      public void writeTo(WriteContext context, Table t) throws IOException {
        write(context, t.id);
        List<Column> columns = t.primaryKey.columns;
        writeInteger(context.out, columns.size());
        for (Column<?> c: columns) {
          write(context, c);
        }
      }
    });

    deserializers.put(Table.class, new Deserializer<Table>() {
      public Table readFrom(ReadContext context, Class<? extends Table> c) throws IOException {
        String id = (String) read(context);
        int columnCount = readInteger(context.in);
        List<Column> columns = new ArrayList<Column>(columnCount);
        for (int i = 0; i < columnCount; ++i) {
          columns.add((Column<?>) read(context));
        }
        return new Table(columns, id);
      }
    });

    serializers.put(Column.class, new Serializer<Column<?>>() {
      public void writeTo(WriteContext context, Column<?> c) throws IOException {
        write(context, c.type);
        write(context, c.id);
      }
    });

    deserializers.put(Column.class, new Deserializer<Column<?>>() {
      public Column<?> readFrom(ReadContext context, Class<? extends Column<?>> c) throws IOException {
        return new Column<Object>((Class) read(context), (String) read(context));
      }
    });
  }

  private static <T> Serializer<T> findSerializer(Class<T> class_) {
    Class<?> c = find(class_, serializers);
    Serializer<?> s = serializers.get(c);
    if (c != class_) {
      synchronized (EpidemicServer.class) {
        Map<Class<?>, Serializer<?>> map = new HashMap<Class<?>, Serializer<?>>(serializers);
        map.put(class_, s);
        serializers = map;
      }
    }
    return (Serializer<T>)s;
  }

  private static <T> Deserializer<T> findDeserializer(Class<T> class_) {
    Class<?> c = find(class_, deserializers);
    Deserializer<?> d = deserializers.get(c);
    if (c != class_) {
      synchronized (EpidemicServer.class) {
        Map<Class<?>, Deserializer<?>> map = new HashMap<Class<?>, Deserializer<?>>(deserializers);
        map.put(class_, d);
        deserializers = map;
      }
    }
    return (Deserializer<T>)d;
  }

  public static void write(WriteContext context, Object value)
    throws IOException
  {
    Integer id = context.objectIDs.get(value);
    if (id == null) {
      Class<?> c = value.getClass();
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

  private static void writeBoolean(OutputStream out, boolean v)
    throws IOException
  {
    out.write(v ? 1 : 0);
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

  private static void writeLong(OutputStream out, long v)
    throws IOException
  {
    if (v == (v & 0x7F)) {
      out.write((int) v);
    } else {
      out.write((int) ((v & 0x7F) | 0x80));
      writeLong(out, v >>> 7);
    }
  }

  private static void writeString(OutputStream out, String s)
    throws IOException
  {
    byte[] bytes = s.getBytes("UTF-8");
    writeInteger(out, bytes.length);
    out.write(bytes);
  }

  private static Class<?> find(Class<?> class_, Map<Class<?>, ?> map) {
    for (Class<?> c = class_; c != Object.class; c = c.getSuperclass()) {
      if (map.containsKey(c)) {
        return c;
      }
    }

    for (Class<?> c: class_.getInterfaces()) {
      if (map.containsKey(c)) {
        return c;
      }
    }

    throw new RuntimeException("no value found for " + class_);
  }

  private static <T> void writeObject(WriteContext context, T v)
    throws IOException
  {
    findSerializer((Class<T>)v.getClass()).writeTo(context, v);
  }

  public static boolean readBoolean(InputStream in)
    throws IOException
  {
    int b = in.read();
    if (b < 0) {
      throw new EOFException();
    } else {
      return b != 0;
    }
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

  public static long readLong(InputStream in)
    throws IOException
  {
    int b = in.read();
    if (b < 0) {
      throw new EOFException();
    } else if ((b & 0x80) == 0) {
      return b;
    } else {
      return (b & 0x7F) | (readLong(in) << 7);
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

  public static <T> T readObject(Class<T> c, ReadContext context)
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
      Class<?> c;
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
      Class<?> value = context.classes.get(id);
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

  public static Object readDefinition(Class<?> c,
                                      ReadContext context)
    throws IOException
  {
    int id = Protocol.readInteger(context.in);
    Object value = readObject(c, context);
    context.objects.put(id, value);
    return value;
  }
}
