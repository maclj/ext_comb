package com.hadoop.plat.common;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.BitSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;

/**
 * m/n=15,且n=100w的情况下，约消耗内存15M。
 * 
 */
public class Bloomfilter implements Serializable {
	
	/**
     * 
     */
    private static final long serialVersionUID = 3906074547543647459L;
    
    private int ver;
    private BitSet bitSet;
	private int bitSetSize;
	private int addedElements;
	private int hashFunctionNumber;

	/**
	 * 构造一个布隆过滤器，过滤器的容量为m * n 个bit.
	 * 
	 * @param m
	 *            当前过滤器预先开辟的最大包含记录。
	 * @param n
	 *            当前过滤器预计所要包含的记录.
	 * @param k
	 *            哈希函数的个数，等同每条记录要占用的bit数.
	 */
	public Bloomfilter(int m, int n, int k) {
		this.hashFunctionNumber = k;
		this.bitSetSize = (int) Math.ceil(m * k * 1.0D);
		this.addedElements = n;
		this.bitSet = new BitSet(this.bitSetSize);
	}
	
	/**
	 * 缺省比例
	 * @param n 预先存入的记录数量
	 */
    public Bloomfilter(int n) {
        this(n * 15, n, 8);
    }

	public void init(List<String> list) {
		for (String lis : list) {
			this.put(lis);
		}
	}
	
	public void init(Set<String> list) {
		for (String lis : list) {
			this.put(lis);
		}
	}

	public void put(String str) {
		int[] positions = createHashes(str.getBytes(), hashFunctionNumber);
		for (int i = 0; i < positions.length; i++) {
			int position = Math.abs(positions[i] % bitSetSize);
			bitSet.set(position, true);
		}
	}

	public boolean contains(String str) {
		byte[] bytes = str.getBytes();
		int[] positions = createHashes(bytes, hashFunctionNumber);
		for (int i : positions) {
			int position = Math.abs(i % bitSetSize);
			if (!bitSet.get(position)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * 得到当前过滤器的错误率.
	 * 
	 * @return
	 */
	public double getFalsePositiveProbability() {
		// (1 - e^(-k * n / m)) ^ k
		return Math.pow(
				(1 - Math.exp(-hashFunctionNumber * (double) addedElements
						/ bitSetSize)), hashFunctionNumber);
	}
	
	public int getVer() {
        return ver;
    }

    public void setVer(int ver) {
        this.ver = ver;
    }

    /**
	 * 将字符串的字节表示进行多哈希编码.
	 * 
	 * @param bytes
	 *            待添加进过滤器的字符串字节表示.
	 * @param hashNumber
	 *            要经过的哈希个数.
	 * @return 各个哈希的结果数组.
	 */
	public static int[] createHashes(byte[] bytes, int hashNumber) {
		int[] result = new int[hashNumber];
		int k = 0;
		while (k < hashNumber) {
			result[k] = HashFunctions.hash(bytes, k);
			k++;
		}
		return result;
	}
	
	
	public static void serialize(String fileName, Bloomfilter obj) {
	    FileOutputStream fos = null;
	    ObjectOutputStream oos = null;
        try {
            fos = new FileOutputStream(fileName);
            oos = new ObjectOutputStream(fos);  
            oos.writeObject(obj);  
            oos.flush();  
            
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(oos != null) {
                IOUtils.closeQuietly(oos);  
            }
            if(fos != null) {
                IOUtils.closeQuietly(fos); 
            }
        }
	}

    public static Bloomfilter deserialize(String fileName) {
        FileInputStream fis = null;  
        ObjectInputStream ois = null;  
         
        try {
            fis = new FileInputStream(fileName);
            ois = new ObjectInputStream(fis);
            Bloomfilter obj = (Bloomfilter) ois.readObject(); 
            return obj;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (ois != null) {
                IOUtils.closeQuietly(ois);
            }
            if (fis != null) {
                IOUtils.closeQuietly(fis);
            }
        }
        return null;
    }
    
    /**
     * 支持InputStream
     * @param in
     * @return
     */
    public static Bloomfilter deserialize(InputStream in) {
        ObjectInputStream ois = null;  
         
        try {
            ois = new ObjectInputStream(in);
            Bloomfilter obj = (Bloomfilter) ois.readObject(); 
            return obj;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (ois != null) {
                IOUtils.closeQuietly(ois);
            }
        }
        return null;
    }
}

class HashFunctions {
	public static int hash(byte[] bytes, int k) {
		switch (k) {
		case 0:
			return RSHash(bytes);
		case 1:
			return JSHash(bytes);
		case 2:
			return ELFHash(bytes);
		case 3:
			return BKDRHash(bytes);
		case 4:
			return APHash(bytes);
		case 5:
			return DJBHash(bytes);
		case 6:
			return SDBMHash(bytes);
		case 7:
			return PJWHash(bytes);
		}
		return 0;
	}

	public static int RSHash(byte[] bytes) {
		int hash = 0;
		int magic = 63689;
		int len = bytes.length;
		for (int i = 0; i < len; i++) {
			hash = hash * magic + bytes[i];
			magic = magic * 378551;
		}
		return hash;
	}

	public static int JSHash(byte[] bytes) {
		int hash = 1315423911;
		for (int i = 0; i < bytes.length; i++) {
			hash ^= ((hash << 5) + bytes[i] + (hash >> 2));
		}
		return hash;
	}

	public static int ELFHash(byte[] bytes) {
		int hash = 0;
		int x = 0;
		int len = bytes.length;
		for (int i = 0; i < len; i++) {
			hash = (hash << 4) + bytes[i];
			if ((x = hash & 0xF0000000) != 0) {
				hash ^= (x >> 24);
				hash &= ~x;
			}
		}
		return hash;
	}

	public static int BKDRHash(byte[] bytes) {
		int seed = 131;
		int hash = 0;
		int len = bytes.length;
		for (int i = 0; i < len; i++) {
			hash = (hash * seed) + bytes[i];
		}
		return hash;
	}

	public static int APHash(byte[] bytes) {
		int hash = 0;
		int len = bytes.length;
		for (int i = 0; i < len; i++) {
			if ((i & 1) == 0) {
				hash ^= ((hash << 7) ^ bytes[i] ^ (hash >> 3));
			} else {
				hash ^= (~((hash << 11) ^ bytes[i] ^ (hash >> 5)));
			}
		}
		return hash;
	}

	public static int DJBHash(byte[] bytes) {
		int hash = 5381;
		int len = bytes.length;
		for (int i = 0; i < len; i++) {
			hash = ((hash << 5) + hash) + bytes[i];
		}
		return hash;
	}

	public static int SDBMHash(byte[] bytes) {
		int hash = 0;
		int len = bytes.length;
		for (int i = 0; i < len; i++) {
			hash = bytes[i] + (hash << 6) + (hash << 16) - hash;
		}
		return hash;
	}

	public static int PJWHash(byte[] bytes) {
		long BitsInUnsignedInt = (4 * 8);
		long ThreeQuarters = ((BitsInUnsignedInt * 3) / 4);
		long OneEighth = (BitsInUnsignedInt / 8);
		long HighBits = (long) (0xFFFFFFFF) << (BitsInUnsignedInt - OneEighth);
		int hash = 0;
		long test = 0;
		for (int i = 0; i < bytes.length; i++) {
			hash = (hash << OneEighth) + bytes[i];
			if ((test = hash & HighBits) != 0) {
				hash = (int) ((hash ^ (test >> ThreeQuarters)) & (~HighBits));
			}
		}
		return hash;
	}
}