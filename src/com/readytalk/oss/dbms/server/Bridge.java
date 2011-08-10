package com.readytalk.oss.dbms.server;

import static com.readytalk.oss.dbms.util.Util.expect;

import com.readytalk.oss.dbms.Table;
import com.readytalk.oss.dbms.Revision;
import com.readytalk.oss.dbms.Revisions;
import com.readytalk.oss.dbms.RevisionBuilder;
import com.readytalk.oss.dbms.DuplicateKeyResolution;
import com.readytalk.oss.dbms.DiffResult;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

public class Bridge {
  private static final int MaxDepth = 16;

  private final Map<RevisionServer, Listener> listeners = new HashMap();

  public Token register(RevisionServer leftServer,
                        Path leftPath,
                        RevisionServer rightServer,
                        Path rightPath)
  {
    if (leftPath.body[0] == null) throw new NullPointerException();

    Mapping left = new Mapping
      (listener(leftServer), leftPath.body, rightServer, rightPath.body);
    
    register(left);

    Mapping right = new Mapping
      (listener(rightServer), rightPath.body, leftServer, leftPath.body);

    register(right);

    return new Token(left, right);
  }

  public void unregister(Token token) {
    unregister(token.left);
    unregister(token.right);
  }

  private void register(Mapping mapping) {
    if (mapping.leftPath[0] == null) throw new NullPointerException();

    mappings(blaze(mapping.leftListener.tree, mapping.leftPath)).add(mapping);
  }

  private void unregister(Mapping mapping) {
    Node n = find(mapping.leftListener.tree, mapping.leftPath);

    n.mappings.remove(mapping);

    if (n.mappings.isEmpty()) {
      delete(mapping.leftListener.tree, mapping.leftPath);

      if (mapping.leftListener.tree.isEmpty()) {
        mapping.leftListener.server.unregisterListener
          (listeners.remove(mapping.leftListener));
      }
    }
  }

  private Listener listener(RevisionServer server) {
    Listener listener = listeners.get(server);
    if (listener == null) {
      listeners.put(server, listener = new Listener(server));
      server.registerListener(listener);
    }
    return listener;
  }

  private Set<Mapping> mappings(Node n) {
    if (n.mappings == null) {
      n.mappings = new HashSet();
    }
    return n.mappings;
  }

  private Node blaze(Map<Comparable, Node> tree, Comparable[] path) {
    Node n = null;
    for (Comparable c: path) {
      if (c == null) throw new NullPointerException();
      n = tree.get(c);
      if (n == null) {
        tree.put(c, n = new Node(c));
      }
      tree = n.tree;
    }
    return n;
  }

  private Node find(Map<Comparable, Node> tree, Comparable[] path) {
    Node n = null;
    for (Comparable c: path) {
      n = tree.get(c);
      tree = n.tree;
    }
    return n;
  }

  private void delete(Map<Comparable, Node> tree, Comparable[] path) {
    Node[] nodes = new Node[path.length];
    Map<Comparable, Node>[] trees = new Map[path.length];
    for (int i = 0; i < path.length; ++i) {
      Node n = tree.get(path[i]);
      if (n == null) {
        return;
      }
      trees[i] = tree;
      nodes[i] = n;
      tree = n.tree;
    }

    for (int i = path.length - 1; i >= 0; --i) {
      trees[i].remove(nodes[i].key);
      if (! trees[i].isEmpty()) {
        break;
      }
    }
  }

  private static void map(Set<Mapping> set, Task task, Object argument) {
    for (Mapping m: set) {
      task.run(m, argument);
    }
  }

  private static void map(Set<Mapping> set, Task task) {
    map(set, task, null);
  }

  public static final class Token {
    private final Mapping left;
    private final Mapping right;

    private Token(Mapping left,
                  Mapping right)
    {
      this.left = left;
      this.right = right;
    }
  }

  public static class Path {
    private final Comparable[] body;

    public Path(Table table, Comparable ... primaryKeyValues) {
      if (table == null) throw new NullPointerException();
      body = new Comparable[primaryKeyValues.length + 1];
      body[0] = table;
      System.arraycopy(primaryKeyValues, 0, body, 1, primaryKeyValues.length);
    }
  }

  private static class Mapping {
    public final Listener leftListener;
    public final Comparable[] leftPath;
    public final RevisionServer rightServer;
    public final Comparable[] rightPath;
    public final Object[] path = new Object[MaxDepth];
    public int depth;
    public Revision base;
    public RevisionBuilder builder;

    public Mapping(Listener leftListener,
                   Comparable[] leftPath,
                   RevisionServer rightServer,
                   Comparable[] rightPath)
    {
      this.leftListener = leftListener;
      this.leftPath = leftPath;
      this.rightServer = rightServer;
      this.rightPath = rightPath;
      this.depth = rightPath.length - 1;
      
      System.arraycopy(rightPath, 0, path, 0, rightPath.length);
    }
  }

  private interface Task {
    public void run(Mapping mapping, Object argument);
  }

  private static class Listener implements Runnable {
    public final RevisionServer server;
    public final Map<Comparable, Node> tree = new HashMap();
    public Revision base = Revisions.Empty;
    public boolean active;

    public static final Task start = new Task() {
        public void run(Mapping mapping, Object argument) {
          mapping.base = mapping.rightServer.head();
          mapping.builder = mapping.base.builder();      
        }
      };

    public static final Task finish = new Task() {
        public void run(Mapping mapping, Object argument) {
          mapping.rightServer.merge(mapping.base, mapping.builder.commit());
          mapping.builder = null;
          expect(mapping.depth == mapping.rightPath.length - 1);
        }
      };

    public static final Task descend = new Task() {
        public void run(Mapping mapping, Object argument) {
          ++ mapping.depth;
        }
      };

    public static final Task ascend = new Task() {
        public void run(Mapping mapping, Object argument) {
          mapping.path[mapping.depth--] = null;
        }
      };
    
    public static final Task insertKey = new Task() {
        public void run(Mapping mapping, Object argument) {
          mapping.builder.insert
            (DuplicateKeyResolution.Overwrite, mapping.path, 0,
             mapping.depth + 1);
        }
      };
    
    public static final Task set = new Task() {
        public void run(Mapping mapping, Object argument) {
          mapping.path[mapping.depth] = argument;
        }
      };
    
    public static final Task delete = new Task() {
        public void run(Mapping mapping, Object argument) {
          mapping.path[mapping.depth] = argument;

          // System.out.println("delete path " + Listener.toString(mapping.path, 0, mapping.depth + 1));

          mapping.builder.delete(mapping.path, 0, mapping.depth + 1);
        }
      };

    public static final Task deleteKey = new Task() {
        public void run(Mapping mapping, Object argument) {
          // System.out.println("delete path " + Listener.toString(mapping.path, 0, mapping.depth));

          mapping.builder.delete(mapping.path, 0, mapping.depth);
        }
      };

    public static final Task insertValue = new Task() {
        public void run(Mapping mapping, Object argument) {
          mapping.path[mapping.depth + 1] = argument;

          // System.out.println("insert path " + Listener.toString(mapping.path, 0, mapping.depth + 2));

          mapping.builder.insert
            (DuplicateKeyResolution.Overwrite, mapping.path, 0,
             mapping.depth + 2);
        }
      };

    private static String toString(Object[] array, int offset, int length) {
      StringBuilder sb = new StringBuilder();
      sb.append("[");
      for (int i = offset; i < offset + length; ++i) {
        sb.append(array[i]);

        if (i + 1 < offset + length) {
          sb.append(" ");
        }
      }
      sb.append("]");
      return sb.toString();
    }
    
    public Listener(RevisionServer server) {
      this.server = server;
    }

    private static void stop(Set<Mapping> allMappings,
                             Set<Mapping>[] mappings,
                             int depth)
    {
      Set<Mapping> ms = mappings[depth];
      if (ms != null) {
        map(ms, finish);
        allMappings.removeAll(ms);
        mappings[depth] = null;
      }
    }

    public void run() {
      if (active) {
        return;
      }

      active = true;

      try {
        Revision head = server.head();
        Set<Mapping>[] mappings = new Set[MaxDepth];
        int depth = 0;
        boolean visitedColumn = true;
        Map<Comparable, Node>[] trees = new Map[MaxDepth + 1];
        final DiffResult result = base.diff(head, true);
        Set<Mapping> allMappings = new HashSet();

        trees[0] = tree;

        while (true) {
          DiffResult.Type type = result.next();
          switch (type) {
          case End:
            stop(allMappings, mappings, depth);

            base = head;
            return;

          case Descend: {
            visitedColumn = true;
            ++ depth;
            map(allMappings, descend);
          } break;

          case Ascend: {
            if (! visitedColumn) {
              visitedColumn = true;
              map(allMappings, insertKey);
            }

            stop(allMappings, mappings, depth);

            trees[depth + 1] = null;
            -- depth;
            map(allMappings, ascend);
          } break;

          case Key: {
            stop(allMappings, mappings, depth);

            Object forkKey = result.fork();
            final Object baseKey = result.base();
            Set<Mapping> newMappings = null;
            Map<Comparable, Node> tree = trees[depth];
            Node node = null;

            if (tree != null) {
              node = tree.get(forkKey == null ? baseKey : forkKey);

              // System.out.println("key " + (forkKey == null ? baseKey : forkKey)
              //                    + " node " + node + " tree " + trees[depth]);

              if (node != null) {
                trees[depth + 1] = node.tree;

                newMappings = node.mappings;
                if (newMappings != null) {
                  expect(! newMappings.isEmpty());
                  mappings[depth] = newMappings;
                  map(newMappings, start);
                }
              }
            }

            if (allMappings.isEmpty() && node == null) {
              result.skip();
            } else {
              if (forkKey != null) {
                if (! visitedColumn) {
                  map(allMappings, insertKey);
                } else {
                  visitedColumn = false;
                }
              
                map(allMappings, set, forkKey);
              } else {
                visitedColumn = true;
                map(allMappings, delete, baseKey);
                if (newMappings != null) {
                  map(newMappings, deleteKey);
                }
                result.skip();
              }
            }

            if (newMappings != null) {
              allMappings.addAll(newMappings);
            }
          } break;

          case Value: {
            visitedColumn = true;
            map(allMappings, insertValue, result.fork());
          } break;

          default:
            throw new RuntimeException("unexpected result type: " + type);
          }
        }
      } finally {
        active = false;
      }
    }
  }

  private static class Node {
    public final Comparable key;
    public final Map<Comparable, Node> tree = new HashMap();
    public Set<Mapping> mappings;

    public Node(Comparable key) {
      this.key = key;
    }
  }
}
