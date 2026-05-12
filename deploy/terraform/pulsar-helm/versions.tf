terraform {
  required_version = ">= 1.4.0"
  required_providers {
    pulsar = {
      source  = "streamnative/pulsar"
      version = ">= 0.9.0"
    }
  }
}
