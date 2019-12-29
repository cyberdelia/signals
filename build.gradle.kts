import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    id("org.jetbrains.kotlin.jvm") version "1.3.61"
    `java-library`

    id("org.jmailen.kotlinter") version "2.2.0"
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

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

kotlinter {
    disabledRules = arrayOf("import-ordering")
}