output "cloud_dns" {
  value = "${aws_instance.cloud.public_dns}"
}

output "edge_dns" {
  value = "${aws_instance.edge.public_dns}"
}
