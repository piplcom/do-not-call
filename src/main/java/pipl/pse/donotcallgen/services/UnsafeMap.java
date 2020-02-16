package pipl.pse.donotcallgen.services;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.math.BigInteger;

public class UnsafeMap implements AutoCloseable {
  private final static float LOAD_FACTOR = 0.7f;
  private Unsafe unsafe;
  private long pointer;
  private long capacity;
  private int entrySize;
  private int numProbs;
  private int hashCollisions = 0;
  private int realCollisions = 0;

  public UnsafeMap(long capacity, int numProbs)
      throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    this.numProbs = numProbs;
    entrySize = 8 + numProbs * 4; //float is 4 bytes
    BigInteger size = BigInteger.valueOf((long) (capacity / LOAD_FACTOR));
    size = size.nextProbablePrime();
    this.capacity = size.longValue();
    allocate(this.capacity * entrySize);
  }

  private void allocate(long bytes)
      throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    Field f = Unsafe.class.getDeclaredField("theUnsafe");
    f.setAccessible(true);
    unsafe = (Unsafe) f.get(null);
    pointer = unsafe.allocateMemory(bytes);
    unsafe.setMemory(pointer, bytes, (byte) 0);
  }

  @Override
  public void close() {
    unsafe.freeMemory(pointer);
  }

  private int hash(long key) {
    return (int) (Math.abs(key) % capacity);
  }

  public boolean put(long key, float... value) {
    boolean isRealCollision = false;
    if (value.length != numProbs) {
      throw new IllegalArgumentException("Invalid number of values for this count map");
    }
    if (value[0] == 0) {
      throw new IllegalArgumentException("0 is not an allowed value in this map.");
    }
    long h = hash(key);
    long pos = h * entrySize + pointer;
    long i = 0;
    while (unsafe.getFloat(pos + 8) != 0) {
      if (unsafe.getLong(pos) == key) {
        realCollisions++;
        isRealCollision = true;
        break;
      } else {
        hashCollisions++;
      }
      i++;
      if (i > capacity) {
        //scanned all slots, should never get here
        throw new RuntimeException("Map over capacity.");
      }
      pos = pointer + entrySize * ((h + i ^ 2) % capacity);
    }
    if (!isRealCollision) {
      unsafe.putLong(pos, key);
      pos += 8;
      for (int j = 0; j < numProbs; j++, pos += 4) {
        unsafe.putFloat(pos, value[j]);
      }
    }

    return isRealCollision;
  }

  public int getHashCollisions() {
    return hashCollisions;
  }

  public int getRealCollisions() {
    return realCollisions;
  }

  public byte getByte(long offset) {
    return unsafe.getByte(pointer + offset);
  }

  public long getSizeInBytes() {
    return capacity * entrySize;
  }

}
