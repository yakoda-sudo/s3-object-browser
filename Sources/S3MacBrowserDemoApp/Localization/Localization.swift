import Foundation

enum Localization {
    static func bundle() -> Bundle {
        let code = UserDefaults.standard.string(forKey: "ui.language") ?? "en"
        for candidate in bundleCandidates() {
            if let path = candidate.path(forResource: code, ofType: "lproj"),
               let bundle = Bundle(path: path) {
                return bundle
            }
        }
        for candidate in bundleCandidates() {
            if let path = candidate.path(forResource: "en", ofType: "lproj"),
               let bundle = Bundle(path: path) {
                return bundle
            }
        }
        return Bundle.main
    }

    static func t(_ key: String) -> String {
        bundle().localizedString(forKey: key, value: key, table: nil)
    }

    private static func bundleCandidates() -> [Bundle] {
        var bundles: [Bundle] = [Bundle.module, Bundle.main]
        let bundleNames = [
            "s3-mac-browser_S3MacBrowserCore",
            "s3-mac-browser_S3MacBrowserDemoApp"
        ]
        for name in bundleNames {
            if let directURL = Bundle.main.url(forResource: name, withExtension: "bundle"),
               let bundle = Bundle(url: directURL) {
                bundles.append(bundle)
            }
        }
        if let resourcesURL = Bundle.main.resourceURL,
           let contents = try? FileManager.default.contentsOfDirectory(at: resourcesURL, includingPropertiesForKeys: nil) {
            for url in contents
                where url.pathExtension == "bundle"
                    && (url.lastPathComponent.contains("S3MacBrowserCore")
                        || url.lastPathComponent.contains("S3MacBrowserDemoApp")) {
                if let bundle = Bundle(url: url) {
                    bundles.append(bundle)
                }
            }
        }
        return bundles
    }
}
