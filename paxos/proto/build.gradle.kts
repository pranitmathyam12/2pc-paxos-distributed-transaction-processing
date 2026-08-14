import com.google.protobuf.gradle.GenerateProtoTask
import com.google.protobuf.gradle.ProtobufExtension

plugins {
    id("java")
    id("com.google.protobuf") version "0.9.4"
    id("idea")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
}

dependencies {
    val grpcVersion = "1.66.0"
    val protobufVersion = "3.25.3"

    implementation("io.grpc:grpc-stub:$grpcVersion")
    implementation("io.grpc:grpc-protobuf:$grpcVersion")
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
    compileOnly("org.apache.tomcat:annotations-api:6.0.53")
}

// --- Configure the protobuf plugin WITHOUT using id("grpc") helper ---
extensions.configure(ProtobufExtension::class.java) {
    // protoc binary
    protoc {
        // use plain property setter to avoid unresolved reference issues
        this.artifact = "com.google.protobuf:protoc:3.25.3"
    }
    // register the grpc codegen plugin
    plugins {
        // create("grpc") works without the id() helper
        create("grpc").apply {
            this.artifact = "io.grpc:protoc-gen-grpc-java:1.66.0"
        }
    }
}

// Wire the grpc plugin to all generateProto tasks
tasks.withType(GenerateProtoTask::class.java).configureEach {
    plugins {
        // again, avoid id("grpc"); use create(...)
        create("grpc")
    }
}

// (Optional) tell IntelliJ where generated sources go
idea {
    module {
        generatedSourceDirs.add(file("build/generated/source/proto/main/java"))
        generatedSourceDirs.add(file("build/generated/source/proto/main/grpc"))
    }
}
