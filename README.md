# aspen-birdcage

This is a collection of tools related to [Aspen](https://www.follettlearning.com/technology/products/student-information-system).

Some novel innovations include:

1. Aspen XR (X-ray) jars. This jar contains classes that resemble actual Aspen classes, but all the inner logic is missing. This means developers can write com.follett.fsc.core.k12.tools.ToolJavaSource classes (imports, exports, reports, procedures) in an IDE like Eclipse. The real-time compiler errors and method autocomplete will help speed up development.
2. BundleWriter. This executable scans this codebase for anything that uses the @Tool annotation and creates the appropriate Aspen tool bundle. The bundles are automatically written in the /target/bundles directory. If the tool source code includes any dependencies to other java files in this project: those are also automatically added to the tool. That helps developers write complex reusable code shared across multiple tools.