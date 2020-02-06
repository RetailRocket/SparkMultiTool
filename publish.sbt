
publishTo := Some("Artifactory Realm" at "http://maven1.int.retailrocket.ru:8081/artifactory/snapshot-local;build.timestamp=" + new java.util.Date().getTime)

credentials += Credentials("Artifactory Realm", "maven1.int.retailrocket.ru", sys.env.getOrElse("MAVEN_REPO_USER", ""), sys.env.getOrElse("MAVEN_REPO_PASS", ""))

