package com.readytalk.revori.util;

import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Collection;
import java.util.Iterator;

public class SetMultimap<K, V> {

  private Map<K, Set<V>> map = new HashMap<K, Set<V>>();
  private int size;

  private static Iterator EMPTY_ITERATOR = new Iterator() {
    public boolean hasNext() {
      return false;
    }

    public Object next() {
      throw new IllegalStateException();
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  };

  private Set<V> create() {
    return new HashSet<V>();
  }

  private Set<V> _get(K key) {
    Set<V> ret = map.get(key);
    if(ret == null) {
      map.put(key, ret = create());
    }
    return ret;
  }

  public void put(K key, V value) {
    if(_get(key).add(value)) {
      size++;
    }
  }

  public Set<V> get(final K key) {
    return new Set<V>() {
      private Set<V> set = null;

      private Set<V> resolve() {
        if(set == null) {
          set = _get(key);
        }
        return set;
      }

      private Set<V> resolveLazy() {
        if(set == null) {
          set = map.get(key);
        }
        return set;
      }

      public boolean add(V e) {
        if(resolve().add(e)) {
          size++;
          return true;
        } else {
          return false;
        }
      }

      public boolean addAll(Collection<? extends V> c) {
        resolve();
        int is = set.size();
        boolean ret = set.addAll(c);
        size += set.size() - is;
        return ret;
      }

      public void clear() {
        if(set != null) {
          size -= set.size();
          set = null;
        }
        map.remove(key);
      }

      public boolean contains(Object o) {
        resolveLazy();
        if(set == null) {
          return false;
        }
        return set.contains(o);
      }

      public boolean containsAll(Collection<?> c){
        resolveLazy();
        if(set == null) {
          return false;
        }
        return set.containsAll(c);
      }

      public boolean equals(Object o) {
        throw new UnsupportedOperationException(); // TODO
      }

      public int hashCode() {
        throw new UnsupportedOperationException(); // TODO
      }

      public boolean isEmpty() {
        resolveLazy();
        if(set == null) {
          return true;
        }
        return set.isEmpty();
      }

      public Iterator<V> iterator() {
        resolveLazy();
        if(set == null) {
          return EMPTY_ITERATOR;
        }
        return set.iterator();
      }

      public boolean remove(Object o) {
        throw new UnsupportedOperationException(); // TODO
      }

      public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException(); // TODO
      }

      public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException(); // TODO
      }

      public int size() {
        resolveLazy();
        if(set == null) {
          return 0;
        }
        return set.size();
      }

      public Object[] toArray() {
        throw new UnsupportedOperationException(); // TODO
      }

      public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException(); // TODO
      }

    };
  }

  public int size() {
    return size;
  }

}