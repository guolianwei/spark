package org.apache.spark;

import com.github.javaparser.StaticJavaParser;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.body.MethodDeclaration;
import com.github.javaparser.ast.visitor.VoidVisitorAdapter;
import com.github.javaparser.resolution.UnsolvedSymbolException;
import com.github.javaparser.symbolsolver.JavaSymbolSolver;
import com.github.javaparser.symbolsolver.resolution.typesolvers.CombinedTypeSolver;
import com.github.javaparser.symbolsolver.resolution.typesolvers.JarTypeSolver;
import com.github.javaparser.symbolsolver.resolution.typesolvers.JavaParserTypeSolver;
import com.github.javaparser.symbolsolver.resolution.typesolvers.ReflectionTypeSolver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class HadoopApiAnalyzer {

    private static Map<String, Set<String>> methodMap = new HashMap<>();

    public static void main(String[] args) throws Exception {
        File projectDir = new File("D:\\srcs\\spark");
        CombinedTypeSolver typeSolver = new CombinedTypeSolver();
        typeSolver.add(new ReflectionTypeSolver()); // 解析JDK类
        typeSolver.add(new JavaParserTypeSolver(new File("D:\\srcs\\spark"))); // 解析项目源码
        JavaSymbolSolver symbolSolver = new JavaSymbolSolver(typeSolver);
        addJarDirectory(typeSolver, "D:\\srcs\\spark\\assembly\\target\\scala-2.12\\jars");
        typeSolver.add(new JarTypeSolver("C:\\Users\\29267\\.m2\\com\\google\\guava\\guava\\33.4.8-jre\\guava-33.4.8-jre.jar"));  // 添加 Guava JAR
        typeSolver.add(new JarTypeSolver("D:\\srcs\\spark\\common\\kvstore\\target\\spark-kvstore_2.12-3.3.5-SNAPSHOT.jar"));  // 添加 Guava JAR

        StaticJavaParser.getConfiguration().setSymbolResolver(symbolSolver);
        analyzeDirectory(projectDir);
        System.out.println("\n=== 方法调用统计 ===");
        methodMap.forEach((key, methods) -> {
            String uniqueMethods = String.join(", ", methods); // 转换为逗号分隔字符串
            System.out.println(key + " : " + uniqueMethods);
        });
    }

    private static void addJarDirectory(CombinedTypeSolver typeSolver, String dirPath) {
        File jarDir = new File(dirPath);
        if (!jarDir.exists() || !jarDir.isDirectory()) return;

        for (File file : jarDir.listFiles()) {
            try {
                if (file.getName().endsWith(".jar")) {
                    typeSolver.add(new JarTypeSolver(file.getAbsolutePath())); // 动态注册JAR解析器[7,9](@ref)
                } else if (file.isDirectory()) {
                    addJarDirectory(typeSolver, file.getAbsolutePath()); // 递归处理子目录
                }
            } catch (IOException e) {
                System.err.println("加载JAR失败: " + file.getAbsolutePath() + " | 原因: " + e.getMessage());
            }
        }
    }
    private static void analyzeDirectory(File dir) throws Exception {
        for (File file : dir.listFiles()) {
            if (file.isDirectory()) {
                analyzeDirectory(file);
            } else if (file.getName().endsWith(".java")) {
                try {
                    CompilationUnit cu = StaticJavaParser.parse(file);
                    new MethodVisitor().visit(cu, null);
                } catch (Exception e) {
                    throw new Exception(e.getMessage() + "::" + file.getAbsolutePath(), e);
                }

            }
        }
    }
    private static class MethodVisitor extends VoidVisitorAdapter<Void> {
        @Override
        public void visit(MethodDeclaration method, Void arg) {
            method.findAll(com.github.javaparser.ast.expr.MethodCallExpr.class).forEach(mce -> {
                try {
                    String packageName = mce.resolve().getPackageName();
                    String className1 = mce.resolve().getClassName();
                    String packageName1 = mce.resolve().getPackageName();
                    if (packageName.startsWith("org.apache.hadoop")) {
                        // 获取方法所在的类名
                        String className = mce.findAncestor(ClassOrInterfaceDeclaration.class)
                                .map(ClassOrInterfaceDeclaration::getNameAsString)
                                .orElse("UnknownClass");
                        String key = packageName1 + "." + className1;

                        // 获取当前方法名
                        String methodName = mce.getNameAsString();
                        System.out.println("类: " + className + " ->" +
                                key + " -> | 方法调用: " +methodName);
                        methodMap.computeIfAbsent(key, k -> new HashSet<>()).add(methodName);

                    }
                } catch (Exception e) {
                    //  System.err.println("解析失败: " + mce + " | 原因: " + e.getMessage());
                }
            });
        }
    }
}