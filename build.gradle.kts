import org.jetbrains.dokka.gradle.DokkaTask
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.3.61"
    `java-library`
    `maven-publish`
    maven
    application

    id("org.jmailen.kotlinter") version "2.2.0"
    id("org.jetbrains.dokka") version "0.10.0"
}

group = "com.lapanthere"

application {
    mainClassName = "com.lapanthere.signals.SignalsKt"
}

repositories {
    jcenter()
}

dependencies {
    val kotlinVersion = "1.3.61"
    implementation(kotlin("stdlib-jdk8", kotlinVersion))
    implementation(kotlin("reflect", kotlinVersion))

    implementation(kotlin("test", kotlinVersion))
    implementation(kotlin("test-junit", kotlinVersion))

    val coroutineVersion = "1.3.3"
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:$coroutineVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:$coroutineVersion")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:$coroutineVersion")

    val awsVersion = "2.10.40"
    implementation("software.amazon.awssdk:s3:$awsVersion")
    testImplementation("software.amazon.awssdk:sts:$awsVersion")

    testImplementation("io.mockk:mockk:1.9.3")
}

publishing {
    repositories {
        maven {
            name = "Github"
            url = uri("https://maven.pkg.github.com/cyberdelia/signals")
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                password = System.getenv("GITHUB_TOKEN")
            }
        }
    }

    publications {
        create<MavenPublication>("default") {
            from(components["java"])
        }
    }
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

tasks.withType<DokkaTask> {
    outputFormat = "gfm"
}

task("pom") {
    doLast {
        maven.pom().writeTo("pom.xml")
    }
}

kotlinter {
    disabledRules = arrayOf("import-ordering")
}
