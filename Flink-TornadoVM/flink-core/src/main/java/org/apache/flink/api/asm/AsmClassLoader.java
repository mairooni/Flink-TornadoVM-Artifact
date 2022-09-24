package org.apache.flink.api.asm;

import java.lang.reflect.Method;

/**
 * Classloader that will be used to load the produced class after the ASM transformation.
 */
public class AsmClassLoader extends ClassLoader {
	public AsmClassLoader() {
		super();
	}

	public Class defineClass(String name, byte[] b) {
		return defineClass(name, b, 0, b.length);
	}

	public MiddleMap loadClass(String className, byte[] b) {
		MiddleMap md = null;
		try {
			Class clazz = null;
			ClassLoader systemLoader = ClassLoader.getSystemClassLoader();
			Class cls = Class.forName("java.lang.ClassLoader");
			Method defineClassMethod = cls.getDeclaredMethod("defineClass", new Class[] { String.class, byte[].class, int.class, int.class });
			// Make protected method accessible
			defineClassMethod.setAccessible(true);
			try {
				Object[] args = new Object[]{className, b, new Integer(0), new Integer(b.length)};
				try {
					clazz = (Class) defineClassMethod.invoke(systemLoader, args);
					md = (MiddleMap) clazz.newInstance();
				} catch (Exception e) {
					Method findLoadedClassMethod = ClassLoader.class.getDeclaredMethod("findLoadedClass", new Class[]{String.class});
					findLoadedClassMethod .setAccessible(true);
					clazz = (Class) findLoadedClassMethod .invoke(systemLoader, className);
					md = (MiddleMap) clazz.newInstance();
				}
			} finally {
				defineClassMethod.setAccessible(false);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		return md;
	}


	public MiddleMap2 loadClass2(String className, byte[] b) {
		MiddleMap2 md = null;
		try {
			Class clazz = null;
			ClassLoader systemLoader = ClassLoader.getSystemClassLoader();
			Class cls = Class.forName("java.lang.ClassLoader");
			Method defineClassMethod = cls.getDeclaredMethod("defineClass", new Class[] { String.class, byte[].class, int.class, int.class });
			// Make protected method accessible
			defineClassMethod.setAccessible(true);
			try {
				Object[] args = new Object[]{className, b, new Integer(0), new Integer(b.length)};
				try {
					clazz = (Class) defineClassMethod.invoke(systemLoader, args);
					md = (MiddleMap2) clazz.newInstance();
				} catch (Exception e) {
					Method findLoadedClassMethod = ClassLoader.class.getDeclaredMethod("findLoadedClass", new Class[]{String.class});
					findLoadedClassMethod .setAccessible(true);
					clazz = (Class) findLoadedClassMethod .invoke(systemLoader, className);
					if (clazz == null) return null;
					md = (MiddleMap2) clazz.newInstance();
				}
			} finally {
				defineClassMethod.setAccessible(false);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		return md;
	}

	public MiddleMap3 loadClass3(String className, byte[] b) {
		MiddleMap3 md = null;
		try {
			Class clazz = null;
			ClassLoader systemLoader = ClassLoader.getSystemClassLoader();
			Class cls = Class.forName("java.lang.ClassLoader");
			Method defineClassMethod = cls.getDeclaredMethod("defineClass", new Class[] { String.class, byte[].class, int.class, int.class });
			// Make protected method accessible
			defineClassMethod.setAccessible(true);
			try {
				Object[] args = new Object[]{className, b, new Integer(0), new Integer(b.length)};
				try {
					clazz = (Class) defineClassMethod.invoke(systemLoader, args);
					md = (MiddleMap3) clazz.newInstance();
				} catch (Exception e) {
					Method findLoadedClassMethod = ClassLoader.class.getDeclaredMethod("findLoadedClass", new Class[]{String.class});
					findLoadedClassMethod .setAccessible(true);
					clazz = (Class) findLoadedClassMethod .invoke(systemLoader, className);
					if (clazz == null) return null;
					md = (MiddleMap3) clazz.newInstance();
				}
			} finally {
				defineClassMethod.setAccessible(false);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		return md;
	}

	public MiddleMap4 loadClass4(String className, byte[] b) {
		MiddleMap4 md = null;
		try {
			Class clazz = null;
			ClassLoader systemLoader = ClassLoader.getSystemClassLoader();
			Class cls = Class.forName("java.lang.ClassLoader");
			Method defineClassMethod = cls.getDeclaredMethod("defineClass", new Class[] { String.class, byte[].class, int.class, int.class });
			// Make protected method accessible
			defineClassMethod.setAccessible(true);
			try {
				Object[] args = new Object[]{className, b, new Integer(0), new Integer(b.length)};
				try {
					clazz = (Class) defineClassMethod.invoke(systemLoader, args);
					md = (MiddleMap4) clazz.newInstance();
				} catch (Exception e) {
					Method findLoadedClassMethod = ClassLoader.class.getDeclaredMethod("findLoadedClass", new Class[]{String.class});
					findLoadedClassMethod .setAccessible(true);
					clazz = (Class) findLoadedClassMethod .invoke(systemLoader, className);
					if (clazz == null) return null;
					md = (MiddleMap4) clazz.newInstance();
				}
			} finally {
				defineClassMethod.setAccessible(false);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		return md;
	}

	public MiddleMap5 loadClass5(String className, byte[] b) {
		MiddleMap5 md = null;
		try {
			Class clazz = null;
			ClassLoader systemLoader = ClassLoader.getSystemClassLoader();
			Class cls = Class.forName("java.lang.ClassLoader");
			Method defineClassMethod = cls.getDeclaredMethod("defineClass", new Class[] { String.class, byte[].class, int.class, int.class });
			// Make protected method accessible
			defineClassMethod.setAccessible(true);
			try {
				Object[] args = new Object[]{className, b, new Integer(0), new Integer(b.length)};
				try {
					clazz = (Class) defineClassMethod.invoke(systemLoader, args);
					md = (MiddleMap5) clazz.newInstance();
				} catch (Exception e) {
					Method findLoadedClassMethod = ClassLoader.class.getDeclaredMethod("findLoadedClass", new Class[]{String.class});
					findLoadedClassMethod .setAccessible(true);
					clazz = (Class) findLoadedClassMethod .invoke(systemLoader, className);
					if (clazz == null) return null;
					md = (MiddleMap5) clazz.newInstance();
				}
			} finally {
				defineClassMethod.setAccessible(false);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		return md;
	}

	public MiddleReduce loadClassRed(String className, byte[] b) {
		MiddleReduce mdr = null;
		try {
			Class clazz = null;
			ClassLoader systemLoader = ClassLoader.getSystemClassLoader();
			Class cls = Class.forName("java.lang.ClassLoader");
			Method defineClassMethod = cls.getDeclaredMethod("defineClass", new Class[] { String.class, byte[].class, int.class, int.class });
			// Make protected method accessible
			defineClassMethod.setAccessible(true);
			try {
				Object[] args = new Object[]{className, b, new Integer(0), new Integer(b.length)};
				try {
					clazz = (Class) defineClassMethod.invoke(systemLoader, args);
					if (clazz == null) return null;
					mdr = (MiddleReduce) clazz.newInstance();
				} catch (Exception e) {
					Method findLoadedClassMethod = ClassLoader.class.getDeclaredMethod("findLoadedClass", new Class[]{String.class});
					findLoadedClassMethod .setAccessible(true);
					clazz = (Class) findLoadedClassMethod .invoke(systemLoader, className);
					if (clazz == null) return null;
					mdr = (MiddleReduce) clazz.newInstance();
				}
			} finally {
				defineClassMethod.setAccessible(false);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		return mdr;
	}

	public MiddleReduce2 loadClassRed2(String className, byte[] b) {
		MiddleReduce2 mdr = null;
		try {
			Class clazz = null;
			ClassLoader systemLoader = ClassLoader.getSystemClassLoader();
			Class cls = Class.forName("java.lang.ClassLoader");
			Method defineClassMethod = cls.getDeclaredMethod("defineClass", new Class[] { String.class, byte[].class, int.class, int.class });
			// Make protected method accessible
			defineClassMethod.setAccessible(true);
			try {
				Object[] args = new Object[]{className, b, new Integer(0), new Integer(b.length)};
				try {
					clazz = (Class) defineClassMethod.invoke(systemLoader, args);
					if (clazz == null) return null;
					mdr = (MiddleReduce2) clazz.newInstance();
				} catch (Exception e) {
					Method findLoadedClassMethod = ClassLoader.class.getDeclaredMethod("findLoadedClass", new Class[]{String.class});
					findLoadedClassMethod .setAccessible(true);
					clazz = (Class) findLoadedClassMethod .invoke(systemLoader, className);
					if (clazz == null) return null;
					mdr = (MiddleReduce2) clazz.newInstance();
				}
			} finally {
				defineClassMethod.setAccessible(false);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		return mdr;
	}

}
