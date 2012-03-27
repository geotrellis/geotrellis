import com.jsuereth.sbtsite.SiteKeys

import com.typesafe.startscript.StartScriptPlugin

seq(site.settings: _*)

seq(ghpages.settings: _*)

git.remoteRepo := "git@github.com:azavea/geotrellis.git"

seq(Revolver.settings: _*)

//mainClass in (Compile, run) := Some("geotrellis.rest.WebRunner")

mainClass := Some("geotrellis.rest.WebRunner")

addCompilerPlugin("com.azavea.math.plugin" %% "optimized-numeric" % "0.1")

//SiteKeys.siteMappings <<=
//  (SiteKeys.siteMappings, PamfletKeys.output) map { (mappings, _, dir) =>
//    mappings ++ (dir ** "*.*" x relativeTo(dir))
//  }

seq(ProguardPlugin.proguardSettings :_*)

seq(StartScriptPlugin.startScriptForClassesSettings: _*)

libraryDependencies += "commons-lang" % "commons-lang" % "2.6"

proguardOptions := Seq(
  "-keepclasseswithmembers public class * { public static void main(java.lang.String[]); }",
  //"-dooptimize",
  "-dontobfuscate",
  "-dontshrink",
  "-dontoptimize",
  """
    -keepclassmembers class * implements java.io.Serializable {
        static long serialVersionUID;
        private void writeObject(java.io.ObjectOutputStream);
        private void readObject(java.io.ObjectInputStream);
        java.lang.Object writeReplace();
        java.lang.Object readResolve();
    }
  """,
  """
-keepclassmembers enum * {
    public static **[] values();
    public static ** valueOf(java.lang.String);
}
  """,
  "-dontnote",
  "-dontwarn",
  //"-allowaccessmodification",
  "-ignorewarnings",
  "-keepdirectories",
  "-keepattributes Exceptions,InnerClasses,Signature,Deprecated,SourceFile,LineNumberTable,*Annotation*,EnclosingMethod",
  "-keep class scala.** { *; }",
  "-keep class akka.** { *; }",
  "-keep class com.typesafe.** { *; }",
  "-keep class ch.** { *; }",
  "-keep class geotrellis.** { *; }",
  "-keep class jline.** { *; }",
  "-keep class com.sun.ws.rs.ext.RuntimeDelegateImpl { *; }",
  "-keep class com.sun.jersey.** { *; }",
  "-keep interface scala.ScalaObject",
  "-keep interface scala.tools.nsc.Interpreter$DebugParam"
)

makeInJarFilter <<= (makeInJarFilter) {
  (makeInJarFilter) => {
    (file) => file match {
      case "commons-beanutils-1.7.0.jar"  => makeInJarFilter(file) + ",!**"
      case "postgis-stubs-1.3.3.jar" => makeInJarFilter(file) + ",!**"
      case "xpp3_min-1.1.4c.jar" => makeInJarFilter(file) + ",!org/xmlpull/v1/XmlPullParser.class"
      case _ => { println("checking " + file.toString) ; makeInJarFilter(file) } 
    }
  }
}

