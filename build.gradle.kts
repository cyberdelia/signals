import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.4.20"
    `java-library`
    `maven-publish`

    id("org.jmailen.kotlinter") version "3.2.0"
    id("org.jetbrains.dokka") version "1.4.10.2"
}

group = "com.lapanthere"

repositories {
    jcenter()
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation(kotlin("test"))
    implementation(kotlin("test-junit"))

    val coroutineVersion = "1.4.2"
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:$coroutineVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:$coroutineVersion")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:$coroutineVersion")

    val awsVersion = "[2.15,)"
    implementation("software.amazon.awssdk:s3:$awsVersion")

    testImplementation("io.mockk:mockk:1.10.2")
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
