
lazy val root = project.in(file("."))
  .settings(
    name := "castool",
    description := "Cassandra tool",
    scalaVersion := "2.13.4",
    libraryDependencies ++= Seq(
      "org.typelevel"                %% "cats-effect"           % Versions.cats,
      "dev.zio"                      %% "zio"                   % Versions.zio,
      "dev.zio"                      %% "zio-streams"           % Versions.zio,
      "dev.zio"                      %% "zio-test"              % Versions.zio % "test",
      "dev.zio"                      %% "zio-test-sbt"          % Versions.zio % "test",
      "dev.zio"                      %% "zio-logging"           % Versions.zioLogging,
      "dev.zio"                      %% "zio-interop-cats"      % Versions.zioInteropCats,
      "org.http4s"                   %% "http4s-blaze-server"   % Versions.http4s,
      "org.http4s"                   %% "http4s-circe"          % Versions.http4s,
      "org.http4s"                   %% "http4s-dsl"            % Versions.http4s,
      "io.circe"                     %% "circe-core"            % Versions.circe,
      "io.circe"                     %% "circe-generic"         % Versions.circe,
      "com.github.pureconfig"        %% "pureconfig"            % Versions.pureconfig,
      "ch.qos.logback"               % "logback-classic"        % "1.2.3",
      "com.datastax.oss"             % "java-driver-core"       % Versions.cassandraDriver,
    ),
  )

  testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")

