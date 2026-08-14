import org.gradle.api.plugins.ApplicationPlugin
import org.gradle.api.tasks.JavaExec

plugins {
    id("java")
    application
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

    implementation("com.opencsv:opencsv:5.9")
    implementation("org.slf4j:slf4j-simple:2.0.13")
}

application {
    mainClass.set("org.paxos.client.ClientLauncher")
}

val demoHost = project.findProperty("paxosNodeHost")?.toString() ?: "127.0.0.1"
val demoTopology = listOf(
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
val demoNodes = project.findProperty("paxosNodes")?.toString()
    ?: demoTopology.joinToString(",") { (id, port) -> "$id=$demoHost:$port" }
val demoCsv = project.findProperty("paxosClientCsv")?.toString() ?: "test.csv"

tasks.register<JavaExec>("runClient") {
    group = ApplicationPlugin.APPLICATION_GROUP
    description = "Runs 10-thread client process"
    mainClass.set("org.paxos.client.ClientLauncher")
    classpath = sourceSets["main"].runtimeClasspath
    args("--csv", demoCsv, "--nodes", demoNodes)
    standardInput = System.`in`
}

tasks.register<JavaExec>("runClientAuto") {
    group = ApplicationPlugin.APPLICATION_GROUP
    description = "Runs client in auto mode (no interactive pauses between sets)"
    mainClass.set("org.paxos.client.ClientLauncher")
    classpath = sourceSets["main"].runtimeClasspath
    args("--csv", demoCsv, "--nodes", demoNodes, "--auto", "true")
    standardInput = System.`in`
}

val benchOpsProp = project.findProperty("benchOps")?.toString() ?: "10000"
val benchReadOnlyProp = project.findProperty("benchReadOnly")?.toString() ?: "50"
val benchCrossProp = project.findProperty("benchCross")?.toString() ?: "50"
val benchSkewProp = project.findProperty("benchSkew")?.toString() ?: "0.0"
val benchSeedProp = project.findProperty("benchSeed")?.toString() ?: "12345"
val benchCsvProp = project.findProperty("benchCsv")?.toString() ?: "client/bench.csv"
val benchClientsProp = project.findProperty("benchClients")?.toString() ?: "32"

tasks.register<JavaExec>("runBench") {
    group = ApplicationPlugin.APPLICATION_GROUP
    description = "Runs client in benchmark mode with generated workload"
    mainClass.set("org.paxos.client.ClientLauncher")
    classpath = sourceSets["main"].runtimeClasspath
    args(
        "--nodes", demoNodes,
        "--auto", "true",
        "--csv", benchCsvProp,
        "--bench", "true",
        "--benchOps", benchOpsProp,
        "--benchReadOnly", benchReadOnlyProp,
        "--benchCross", benchCrossProp,
        "--benchSkew", benchSkewProp,
        "--benchSeed", benchSeedProp,
        "--benchClients", benchClientsProp,
    )
    standardInput = System.`in`
}

tasks.withType<JavaExec>().configureEach {
    if (standardInput == null) {
        standardInput = System.`in`
    }
}
