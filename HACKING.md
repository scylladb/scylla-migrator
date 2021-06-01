The migrator can have debug logging enabled inside connector classes e.g.

`cd spark-cassandra-connector`

```
diff --git a/connector/src/it/resources/log4j.properties b/connector/src/it/resources/log4j.properties
index fc9d4ad8..a727a5ff 100644
--- a/connector/src/it/resources/log4j.properties
+++ b/connector/src/it/resources/log4j.properties
@@ -42,3 +42,5 @@ log4j.logger.org.eclipse.jetty.server.Server=ERROR

 #See CCM Bridge INFO
 log4j.logger.com.datastax.spark.connector.ccm=INFO
+
+#log4j.logger.com.datastax.spark.connector.writer=DEBUG
diff --git a/connector/src/it/resources/logback.xml b/connector/src/it/resources/logback.xml
index a54a45b0..9e7c98f2 100644
--- a/connector/src/it/resources/logback.xml
+++ b/connector/src/it/resources/logback.xml
@@ -15,7 +15,7 @@
         </encoder>
     </appender>

-    <root level="warn">
+    <root level="DEBUG">
         <appender-ref ref="STDOUT" />
     </root>
```

Other levels can be used, e.g. INFO

build with such changes will print debug messages,
these files can also be injected by app, so you don't have to build, if passed on properly (how?)

