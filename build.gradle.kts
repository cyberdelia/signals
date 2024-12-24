import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "2.0.21"
    `java-library`
    `maven-publish`

    id("org.jmailen.kotlinter") version "4.3.0"
    id("org.jetbrains.dokka") version "1.9.20"
}

group = "com.lapanthere"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))

    implementation(platform("org.jetbrains.kotlinx:kotlinx-coroutines-bom:1.10.1"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactive")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test")

    implementation(platform("software.amazon.awssdk:bom:2.29.35"))
    implementation("software.amazon.awssdk:s3")
    testImplementation("software.amazon.awssdk:sts")

    testImplementation("io.mockk:mockk:1.13.14")
}

tasks.register<Jar>("sourcesJar") {
    archiveClassifier.set("sources")
    from(sourceSets.main.get().allSource)
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

kotlin {
    explicitApi()
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
