import org.gradle.api.plugins.ApplicationPlugin
import org.gradle.api.tasks.JavaExec

plugins {
    id("java")
    application
}

val migrNew = project.findProperty("migrNew")?.toString() ?: "client/shard-map.new.json"
val migrOld = project.findProperty("migrOld")?.toString() ?: "client/shard-map.json"

tasks.register<JavaExec>("runOfflineMigrator") {
    group = ApplicationPlugin.APPLICATION_GROUP
    description = "Runs the offline migrator to apply a reshard plan"
    mainClass.set("org.paxos.tools.OfflineMigrator")
    classpath = sourceSets["main"].runtimeClasspath
    args(migrNew, migrOld)
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

dependencies {
    val grpcVersion = "1.66.0"

    implementation(project(":proto"))
    implementation("io.grpc:grpc-netty-shaded:$grpcVersion")
    implementation("io.grpc:grpc-stub:$grpcVersion")
    implementation("io.grpc:grpc-protobuf:$grpcVersion")

    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.2")
    implementation("org.slf4j:slf4j-simple:2.0.13")
    implementation("org.mapdb:mapdb:3.0.10")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.3")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.3")
}

tasks.test {
    useJUnitPlatform()
}

application {
    mainClass.set("org.paxos.node.NodeLauncher")
}

val nodeHost = project.findProperty("paxosNodeHost")?.toString() ?: "127.0.0.1"
val nodeTopology = listOf(
    "n1" to 51051,
    "n2" to 51052,
    "n3" to 51053,
    "n4" to 51054,
    "n5" to 51055,
    "n6" to 51056,
    "n7" to 51057,
    "n8" to 51058,
    "n9" to 51059,
)

fun registerNodeTask(name: String, nodeId: String, port: Int) {
    fun clusterIndex(id: String): Int {
        val digits = id.filter { it.isDigit() }
        val n = digits.toIntOrNull() ?: return -1
        return (n - 1) / 3
    }

    val myCluster = clusterIndex(nodeId)
    val peersCsv = nodeTopology
        .filter { (id, _) -> id != nodeId && clusterIndex(id) == myCluster }
        .joinToString(",") { (peerId, peerPort) -> "$peerId=$nodeHost:$peerPort" }

    tasks.register<JavaExec>(name) {
        group = ApplicationPlugin.APPLICATION_GROUP
        description = "Runs $nodeId using the predefined local topology"
        mainClass.set("org.paxos.node.NodeLauncher")
        classpath = sourceSets["main"].runtimeClasspath
        args(nodeId, port.toString(), peersCsv)
        environment("PAXOS_NODE_HOST", nodeHost)
        val fullTopo = nodeTopology.joinToString(",") { (id, p) -> "$id=$nodeHost:$p" }
        environment("PAXOS_FULL_TOPOLOGY", fullTopo)
    }
}

registerNodeTask("runNode1", "n1", 51051)
registerNodeTask("runNode2", "n2", 51052)
registerNodeTask("runNode3", "n3", 51053)
registerNodeTask("runNode4", "n4", 51054)
registerNodeTask("runNode5", "n5", 51055)
registerNodeTask("runNode6", "n6", 51056)
registerNodeTask("runNode7", "n7", 51057)
registerNodeTask("runNode8", "n8", 51058)
registerNodeTask("runNode9", "n9", 51059)


tasks.register<JavaExec>("runNodes") {
    group = ApplicationPlugin.APPLICATION_GROUP
    description = "Runs all nine nodes"
    mainClass.set("org.paxos.node.ClusterRunner")
    classpath = sourceSets["main"].runtimeClasspath
    standardInput = System.`in`
}

val paxosGc = project.findProperty("paxosGc")?.toString()?.lowercase() ?: "g1"
val paxosHeap = project.findProperty("paxosHeap")?.toString() ?: "1024m"

tasks.withType<JavaExec>().configureEach {
    val gcArgs = when (paxosGc) {
        "zgc" -> listOf("-XX:+UseZGC", "-XX:+ZGenerational")
        else -> listOf("-XX:+UseG1GC", "-XX:MaxGCPauseMillis=100")
    }
    jvmArgs(gcArgs)
    jvmArgs("-Xms$paxosHeap", "-Xmx$paxosHeap", "-XX:+AlwaysPreTouch")
    val paxosJfr = (project.findProperty("paxosJfr")?.toString()?.lowercase() == "true")
    if (paxosJfr) {
        doFirst { file("jfr").mkdirs() }
        val recording = "filename=jfr/${this.name}-%t.jfr,settings=profile,dumponexit=true"
        jvmArgs("-XX:StartFlightRecording=$recording")
    }
}
