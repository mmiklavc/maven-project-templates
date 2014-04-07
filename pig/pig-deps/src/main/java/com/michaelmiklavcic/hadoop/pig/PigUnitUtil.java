package com.michaelmiklavcic.hadoop.pig;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.NotFoundException;

/**
 * Credit for initial skeleton for this fix goes to -
 * https://gist.github.com/mwacc/5599123#file-fixhadooponwindows-java
 */
public class PigUnitUtil {

    /**
     * Fix the followind Hadoop problem on Windows: 1) mapReduceLayer.Launcher:
     * Backend error message during job submission java.io.IOException: Failed
     * to set permissions of path: \tmp\hadoop-MyUsername\mapred\staging\ 2)
     * java.io.IOException: Failed to set permissions of path:
     * bla-bla-bla\.staging to 0700
     */

    public static void runFix() throws NotFoundException, CannotCompileException {
        if (isWindows()) { // run fix only on Windows
            setUpSystemVariables();
            fixCheckReturnValueMethod();
        }
    }

    // set up correct temporary directory on windows
    private static void setUpSystemVariables() {
        System.getProperties().setProperty("java.io.tmpdir", "/TMP/");
    }

    /**
     * org.apache.hadoop.fs.FileUtil#checkReturnValue doesn't work on Windows at
     * all so, let's change method body with Javassist on empty body
     */
    private static void fixCheckReturnValueMethod() throws NotFoundException, CannotCompileException {
        ClassPool cp = new ClassPool(true);
        {
            CtClass ctClass = cp.get("org.apache.hadoop.fs.FileUtil");
            {
                CtMethod ctMethod = ctClass.getDeclaredMethod("checkReturnValue");
                ctMethod.setBody("{  }");
            }
            {
                CtMethod ctMethod = ctClass.getDeclaredMethod("canRead");
                ctMethod.setBody("{ return true; }");
            }
            {
                CtMethod ctMethod = ctClass.getDeclaredMethod("canWrite");
                ctMethod.setBody("{ return true; }");
            }
            {
                CtMethod ctMethod = ctClass.getDeclaredMethod("canExecute");
                ctMethod.setBody("{ return true; }");
            }
            toClass(ctClass);
        }
        {
            CtClass ctClass = cp.get("org.apache.hadoop.fs.RawLocalFileSystem");
            CtMethod ctMethod = ctClass.getDeclaredMethod("setPermission");
            ctMethod.setBody("{  }");
            toClass(ctClass);
        }
        {
            CtClass ctClass = cp.get("org.apache.hadoop.yarn.util.WindowsBasedProcessTree");
            CtMethod ctMethod = ctClass.getDeclaredMethod("isAvailable");
            ctMethod.setBody("{ return true; }");
            toClass(ctClass);
        }
        {
            CtClass ctClass = cp.get("org.apache.hadoop.util.Shell");
            CtMethod ctMethod = ctClass.getDeclaredMethod("getWinUtilsPath");
            ctMethod.setBody("{ return \"\"; }");
            toClass(ctClass);
        }
        {
            CtClass ctClass = cp.get("org.apache.hadoop.fs.RawLocalFileSystem$RawLocalFileStatus");
            {
                CtMethod ctMethod = ctClass.getDeclaredMethod("getPermission");
                ctMethod.setBody("{ return new org.apache.hadoop.fs.permission.FsPermission((short)777);); }");
            }
            {
                CtMethod ctMethod = ctClass.getDeclaredMethod("getOwner");
                ctMethod.setBody("{ return \"\" }");
            }
            {
                CtMethod ctMethod = ctClass.getDeclaredMethod("getGroup");
                ctMethod.setBody("{ return \"\" }");
            }
            toClass(ctClass);
        }
    }

    private static void toClass(CtClass ctClass) throws CannotCompileException {
        try {
            ctClass.toClass();
        } catch (CannotCompileException e) {
            if (!LinkageError.class.equals(e.getCause().getClass())) {
                throw e;
            }
        }
    }

    private static boolean isWindows() {
        String OS = System.getProperty("os.name");
        return OS.startsWith("Windows");
    }

}
