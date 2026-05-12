terraform {
  required_version = ">= 1.4.0"
  # No provider dependencies — this module is pure null_resource + local-exec because
  # the streamnative/pulsar provider's pulsar_function resource only accepts local jar
  # paths, and pulsar_package does not exist as a resource. The wrapper using this
  # module still depends on the provider for orchestrator-topology.
}
