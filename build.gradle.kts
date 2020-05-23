import org.jetbrains.dokka.gradle.DokkaTask
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.3.72"
    `java-library`
    `maven-publish`

    id("org.jmailen.kotlinter") version "2.3.2"
    id("org.jetbrains.dokka") version "0.10.1"
}

group = "com.lapanthere"

repositories {
    jcenter()
}

dependencies {
    val kotlinVersion = "1.3.72"
    implementation(kotlin("stdlib-jdk8", kotlinVersion))
    implementation(kotlin("reflect", kotlinVersion))

    implementation(kotlin("test", kotlinVersion))
    implementation(kotlin("test-junit", kotlinVersion))

    val coroutineVersion = "1.3.7"
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:$coroutineVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:$coroutineVersion")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:$coroutineVersion")

    val awsVersion = "2.13.23"
    implementation("software.amazon.awssdk:s3:$awsVersion")
    testImplementation("software.amazon.awssdk:sts:$awsVersion")

    testImplementation("io.mockk:mockk:1.10.0")
}

tasks.register<Jar>("sourcesJar") {
    archiveClassifier.set("sources")
    from(sourceSets.main.get().allSource)
}

tasks.named<KotlinCompile>("compileKotlin") {
    kotlinOptions.freeCompilerArgs = listOf("-Xexplicit-api=strict")
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

tasks.withType<DokkaTask> {
    outputFormat = "gfm"
}

kotlinter {
    disabledRules = arrayOf("import-ordering")
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

        maven {
            name = "Bintray"
            url = uri("https://api.bintray.com/maven/cyberdelia/lapanthere/signals/;publish=1")
            credentials {
                username = System.getenv("BINTRAY_USERNAME")
                password = System.getenv("BINTRAY_TOKEN")
            }
        }
    }

    publications {
        create<MavenPublication>("default") {
            from(components["java"])
            artifact(tasks["sourcesJar"])
            pom {
                name.set("Signals")
                description.set("S3 Streaming Client")
                url.set("https://github.com/cyberdelia/signals")
                scm {
                    connection.set("scm:git:git://github.com/cyberdelia/signals.git")
                    developerConnection.set("scm:git:ssh://github.com/cyberdelia/signals.git")
                    url.set("https://github.com/cyberdelia/signals")
                }
            }
        }
    }
}
