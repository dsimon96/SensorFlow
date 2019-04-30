variable "aws_region" {
  description = "AWS region to launch servers."
  default     = "us-east-2"
}

variable "public_key_path" {
  description = "Path to local public key for SSH authentication"
  default     = "~/.ssh/id_rsa.pub"
}

variable "private_key_path" {
  description = "Path to local private key for SSH authentication"
  default     = "~/.ssh/id_rsa"
}

variable "key_name" {
  description = "Desired name of AWS key pair"
  default     = "sensorflow-tf"
}
