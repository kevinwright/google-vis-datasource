import sbt._
import de.element34.sbteclipsify._

class VisualizationDatasourceProject(info: ProjectInfo) extends DefaultProject(info)
with IdeaProject
with Eclipsify
//with posterous.Publish
{
  override def compileOptions = Seq(Deprecation, Unchecked)
  override def javaCompileOptions = javaCompileOptions("-Xlint:deprecation")//, "-Xlint:unchecked")

  //Dependencies
  //val scalap = "org.scala-lang" % "scalap" % "2.8.1" withSources()
  //val specs = "org.scala-tools.testing" %% "specs" % "1.6.6"
  val unit = "junit" % "junit" % "4.8.2" % "test"
  //val logback = "ch.qos.logback" % "logback-classic" % "0.9.25"
  //val grizzledSlf4j = "org.clapper" %% "grizzled-slf4j" % "0.3.2"
  
  val commonsLang = "commons-lang" % "commons-lang" % "2.4"
  val commonsLogging = "commons-logging" % "commons-logging" % "1.1.1"
  val guava = "com.google.guava" % "guava" % "r07"
  val icu4j = "com.ibm.icu" % "icu4j" % "4.0.1"
  val opencsv = "net.sf.opencsv" % "opencsv" % "1.8"
  val servlet = "javax.servlet" % "servlet-api" % "2.5"
  val easymock = "org.easymock" % "easymock" % "2.5" % "test"

  // Publishing
//  override def managedStyle = ManagedStyle.Maven
//  val publishTo = "Scala Tools Nexus" at "http://nexus.scala-tools.org/content/repositories/snapshots/"
//  Credentials(Path.userHome / ".ivy2" / ".credentials", log)
//  override def publishAction = super.publishAction && publishCurrentNotes
//  override def extraTags = "scalaj" :: super.extraTags

}
