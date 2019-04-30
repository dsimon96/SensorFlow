provider "aws" {
  region                  = "${var.aws_region}"
  shared_credentials_file = "~/.aws/tf_credentials"
}

resource "aws_default_vpc" "default" {
  enable_dns_hostnames = "true"
}

resource "aws_default_subnet" "default" {
  availability_zone       = "us-east-2b"
  map_public_ip_on_launch = "true"
}

resource "aws_security_group" "default" {
  name        = "sensorflow_sg"
  description = "provide SSH and HTTP access to instances"
  vpc_id      = "${aws_default_vpc.default.id}"

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # allow all outbound traffic
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_key_pair" "auth" {
  key_name   = "${var.key_name}"
  public_key = "${file(var.public_key_path)}"
}

resource "aws_instance" "edge" {
  connection {
    type  = "ssh"
    user  = "ubuntu"
    private_key = "${file(var.private_key_path)}"
  }

  instance_type     = "a1.medium"
  ami               = "ami-0f2057f28f0a44d06"
  availability_zone = "us-east-2b"

  key_name = "${aws_key_pair.auth.id}"

  vpc_security_group_ids = ["${aws_security_group.default.id}"]
  subnet_id              = "${aws_default_subnet.default.id}"

  provisioner "remote-exec" {}
}

resource "aws_instance" "cloud" {
  connection {
    type  = "ssh"
    user  = "ubuntu"
    private_key = "${file(var.private_key_path)}"
  }

  instance_type     = "c5.xlarge"
  ami               = "ami-0c55b159cbfafe1f0"
  availability_zone = "us-east-2b"

  key_name = "${aws_key_pair.auth.id}"

  vpc_security_group_ids = ["${aws_security_group.default.id}"]
  subnet_id              = "${aws_default_subnet.default.id}"

  provisioner "remote-exec" {}
}
