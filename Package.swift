// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "s3-mac-browser",
    defaultLocalization: "en",
    platforms: [
        .macOS(.v13)
    ],
    products: [
        .library(name: "S3MacBrowserCore", targets: ["S3MacBrowserCore"])
    ],
    targets: [
        .target(
            name: "S3MacBrowserCore",
            path: "Sources/S3MacBrowserDemoApp",
            exclude: ["MetricsTests.swift"],
            resources: [
                .process("Resources")
            ]
        ),
        .testTarget(
            name: "S3MacBrowserDemoAppTests",
            dependencies: ["S3MacBrowserCore"],
            path: "Sources/S3MacBrowserDemoApp",
            exclude: ["Localization", "Models", "Services", "ViewModels", "Views", "S3MacBrowserDemoApp.swift"],
            sources: ["MetricsTests.swift"]
        )
    ]
)
