---
layout: operations
title: Introduction

tutorial: operations
num: 1
outof: 5
---

#### What are Operations?

A geoprocessing model in GeoTrellis is composed of smaller geoprocessing
operations with  well-defined inputs and outputs.  The next section describes
how to create your own operations, but it is usually better to compose an
operation out of existing operations if that is possible.  The following is a
list of some of the operations available.  Operations in italics are planned
for the future.

The GeoTrellis naming convention for operations namespaces every operation
within a single package, and we commonly refer to the operation with the
package name in the format `package.operation`.  For example, data loading
operations are in the `io` package, and so the `LoadRaster` operation is
referred to as `io.LoadRaster`.
