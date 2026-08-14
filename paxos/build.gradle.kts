plugins {
    // no plugins at root; apply per-module
}

allprojects {
    repositories {
        mavenCentral()
    }
}

subprojects {
    group = "org.paxos"
    version = "0.1.0"
}